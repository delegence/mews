use std::time::Duration;

use futures_util::StreamExt;
use reqwest::{RequestBuilder, Response, StatusCode};
use serde::de::DeserializeOwned;

use crate::{ProviderError, ProviderResult};

const MAX_ERROR_BODY_BYTES: usize = 64 * 1024;
const MAX_RESPONSE_BODY_BYTES: usize = 8 * 1024 * 1024;

pub(crate) async fn send_with_retry(request: RequestBuilder) -> ProviderResult<Response> {
    const ATTEMPTS: usize = 3;
    for attempt in 0..ATTEMPTS {
        let call = request.try_clone().ok_or_else(|| {
            ProviderError::InvalidRequest("provider request body cannot be retried".into())
        })?;
        let response = match call.send().await {
            Ok(response) => response,
            Err(error) if attempt + 1 < ATTEMPTS && error.is_connect() => {
                tokio::time::sleep(Duration::from_secs(1_u64 << attempt)).await;
                continue;
            }
            Err(error) => return Err(ProviderError::Http(error.to_string())),
        };
        let status = response.status();
        let retry_after = response
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok());
        if status.is_success() {
            return Ok(response);
        }
        let retryable = matches!(
            status,
            StatusCode::TOO_MANY_REQUESTS
                | StatusCode::BAD_GATEWAY
                | StatusCode::SERVICE_UNAVAILABLE
                | StatusCode::GATEWAY_TIMEOUT
        );
        if retryable && attempt + 1 < ATTEMPTS {
            let delay = retry_after.unwrap_or(1_u64 << attempt).min(30);
            tokio::time::sleep(Duration::from_secs(delay)).await;
            continue;
        }
        if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
            return Err(ProviderError::Authentication(response_text(response).await));
        }
        if status == StatusCode::TOO_MANY_REQUESTS {
            return Err(ProviderError::RateLimited { retry_after });
        }
        return Err(ProviderError::Http(format!(
            "HTTP {status}: {}",
            response_text(response).await
        )));
    }
    unreachable!("retry loop always returns")
}

async fn response_text(response: Response) -> String {
    response_text_limited(response, MAX_ERROR_BODY_BYTES)
        .await
        .unwrap_or_else(|error| error.to_string())
}

pub(crate) async fn response_json<T: DeserializeOwned>(response: Response) -> ProviderResult<T> {
    let bytes = response_bytes_limited(response, MAX_RESPONSE_BODY_BYTES).await?;
    serde_json::from_slice(&bytes)
        .map_err(|error| ProviderError::InvalidResponse(error.to_string()))
}

pub(crate) async fn response_text_limited(
    response: Response,
    limit: usize,
) -> ProviderResult<String> {
    let bytes = response_bytes_limited(response, limit).await?;
    String::from_utf8(bytes).map_err(|error| {
        ProviderError::InvalidResponse(format!("provider response is not UTF-8: {error}"))
    })
}

async fn response_bytes_limited(response: Response, limit: usize) -> ProviderResult<Vec<u8>> {
    let mut bytes = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| ProviderError::Http(error.to_string()))?;
        if bytes.len().saturating_add(chunk.len()) > limit {
            return Err(ProviderError::InvalidResponse(format!(
                "provider response exceeds {limit} bytes"
            )));
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;

    #[tokio::test]
    async fn retries_rate_limits_and_returns_success() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let server_calls = calls.clone();
        let server = tokio::spawn(async move {
            for _ in 0..2 {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = [0_u8; 2048];
                let _ = stream.read(&mut request).await.unwrap();
                let call = server_calls.fetch_add(1, Ordering::SeqCst);
                let response = if call == 0 {
                    "HTTP/1.1 429 Too Many Requests\r\nRetry-After: 0\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                } else {
                    "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok"
                };
                stream.write_all(response.as_bytes()).await.unwrap();
            }
        });
        let response = send_with_retry(reqwest::Client::new().get(format!("http://{address}")))
            .await
            .unwrap();
        assert_eq!(response.text().await.unwrap(), "ok");
        server.await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn does_not_retry_an_ambiguous_response_timeout() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let server_calls = calls.clone();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            server_calls.fetch_add(1, Ordering::SeqCst);
            let mut request = [0_u8; 2048];
            let _ = stream.read(&mut request).await.unwrap();
            tokio::time::sleep(Duration::from_millis(200)).await;
        });
        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(30))
            .build()
            .unwrap();

        let result = send_with_retry(client.post(format!("http://{address}")).body("work")).await;

        assert!(matches!(result, Err(ProviderError::Http(_))));
        server.await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn caps_provider_error_bodies() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 2048];
            let _ = stream.read(&mut request).await.unwrap();
            let body = vec![b'x'; MAX_ERROR_BODY_BYTES + 1];
            let headers = format!(
                "HTTP/1.1 400 Bad Request\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(headers.as_bytes()).await.unwrap();
            stream.write_all(&body).await.unwrap();
        });

        let error = send_with_retry(reqwest::Client::new().get(format!("http://{address}")))
            .await
            .unwrap_err();

        assert!(error.to_string().contains("exceeds 65536 bytes"));
        server.await.unwrap();
    }
}
