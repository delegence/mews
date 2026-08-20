use futures_util::StreamExt;
use reqwest::{Client, RequestBuilder, Response, StatusCode};
use serde::de::DeserializeOwned;
use std::time::Duration;

use crate::{ProviderError, ProviderResult};

const MAX_ERROR_BODY_BYTES: usize = 64 * 1024;
const MAX_RESPONSE_BODY_BYTES: usize = 8 * 1024 * 1024;
const PROVIDER_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const PROVIDER_IDLE_READ_TIMEOUT: Duration = Duration::from_secs(5 * 60);

pub(crate) fn client() -> Client {
    client_with_timeouts(PROVIDER_CONNECT_TIMEOUT, PROVIDER_IDLE_READ_TIMEOUT)
}

fn client_with_timeouts(connect_timeout: Duration, read_timeout: Duration) -> Client {
    // Reqwest resets this read deadline after each frame, so active streams have no total limit.
    Client::builder()
        .connect_timeout(connect_timeout)
        .read_timeout(read_timeout)
        .build()
        .expect("provider HTTP client configuration is valid")
}

pub(crate) async fn send_with_retry(request: RequestBuilder) -> ProviderResult<Response> {
    let response = request
        .send()
        .await
        .map_err(|error| ProviderError::Http(error.to_string()))?;
    let status = response.status();
    let retry_after = response
        .headers()
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok());
    if status.is_success() {
        return Ok(response);
    }
    if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
        return Err(ProviderError::Authentication(response_text(response).await));
    }
    if status == StatusCode::TOO_MANY_REQUESTS {
        return Err(ProviderError::RateLimited { retry_after });
    }
    let body = response_text(response).await;
    if matches!(status, StatusCode::BAD_REQUEST | StatusCode::NOT_FOUND)
        && definitive_cursor_rejection(&body)
    {
        return Err(ProviderError::CursorRejected(body));
    }
    Err(ProviderError::Http(format!("HTTP {status}: {body}")))
}

fn definitive_cursor_rejection(body: &str) -> bool {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(body) else {
        return false;
    };
    matches!(
        value
            .pointer("/error/code")
            .and_then(serde_json::Value::as_str),
        Some(
            "previous_response_not_found"
                | "response_not_found"
                | "invalid_previous_response_id"
                | "expired_response"
        )
    )
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
    use std::time::Duration;

    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;

    #[tokio::test]
    async fn ambiguous_http_responses_are_not_retried() {
        for status in [429, 502, 503, 504] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let calls = Arc::new(AtomicUsize::new(0));
            let server_calls = calls.clone();
            let server = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = [0_u8; 2048];
                let _ = stream.read(&mut request).await.unwrap();
                server_calls.fetch_add(1, Ordering::SeqCst);
                let response = format!(
                    "HTTP/1.1 {status} Error\r\nRetry-After: 7\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                );
                stream.write_all(response.as_bytes()).await.unwrap();
            });

            let error = send_with_retry(
                reqwest::Client::new()
                    .post(format!("http://{address}"))
                    .body("work"),
            )
            .await
            .unwrap_err();

            if status == 429 {
                assert!(matches!(
                    error,
                    ProviderError::RateLimited {
                        retry_after: Some(7)
                    }
                ));
            } else {
                assert!(matches!(error, ProviderError::Http(_)));
            }
            server.await.unwrap();
            assert_eq!(calls.load(Ordering::SeqCst), 1);
        }
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

    #[tokio::test]
    async fn idle_response_body_times_out_and_closes_connection() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 2048];
            let _ = stream.read(&mut request).await.unwrap();
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 2\r\n\r\n",
                )
                .await
                .unwrap();
            let mut byte = [0_u8; 1];
            stream.read(&mut byte).await
        });
        let response = send_with_retry(
            client_with_timeouts(Duration::from_secs(1), Duration::from_millis(50))
                .get(format!("http://{address}")),
        )
        .await
        .unwrap();

        let error = response_json::<serde_json::Value>(response)
            .await
            .unwrap_err();

        assert!(matches!(error, ProviderError::Http(_)));
        let closed = tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .unwrap()
            .unwrap();
        assert!(
            matches!(closed, Ok(0))
                || matches!(
                    closed,
                    Err(ref error)
                        if matches!(
                            error.kind(),
                            std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe
                        )
                ),
            "provider connection remained open: {closed:?}"
        );
    }
}
