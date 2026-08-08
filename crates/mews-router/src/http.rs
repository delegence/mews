use std::time::Duration;

use reqwest::{RequestBuilder, Response, StatusCode};

use crate::{ProviderError, ProviderResult};

pub(crate) async fn send_with_retry(request: RequestBuilder) -> ProviderResult<Response> {
    const ATTEMPTS: usize = 3;
    for attempt in 0..ATTEMPTS {
        let call = request.try_clone().ok_or_else(|| {
            ProviderError::InvalidRequest("provider request body cannot be retried".into())
        })?;
        let response = match call.send().await {
            Ok(response) => response,
            Err(error) if attempt + 1 < ATTEMPTS && (error.is_connect() || error.is_timeout()) => {
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
        let retryable = status == StatusCode::TOO_MANY_REQUESTS
            || status == StatusCode::REQUEST_TIMEOUT
            || status.is_server_error();
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
    response
        .text()
        .await
        .unwrap_or_else(|_| "response body unavailable".into())
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
}
