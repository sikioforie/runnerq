/// Error type for ? operator compatibility with ActivityResult
#[derive(Debug)]
pub enum ActivityError {
    Retry(String),
    NonRetry(String),
}

impl std::fmt::Display for ActivityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ActivityError::Retry(msg) => write!(f, "Retryable error: {}", msg),
            ActivityError::NonRetry(msg) => write!(f, "Non-retryable error: {}", msg),
        }
    }
}

impl std::error::Error for ActivityError {}

/// Trait to determine if an error should be retried
pub trait RetryableError {
    fn is_retryable(&self) -> bool;
}

// Implement RetryableError for common error types

/// Implementation for std::io::Error
impl RetryableError for std::io::Error {
    fn is_retryable(&self) -> bool {
        match self.kind() {
            std::io::ErrorKind::TimedOut => true,
            std::io::ErrorKind::Interrupted => true,
            std::io::ErrorKind::WouldBlock => true,
            std::io::ErrorKind::ConnectionRefused => true,
            std::io::ErrorKind::ConnectionAborted => true,
            std::io::ErrorKind::ConnectionReset => true,
            std::io::ErrorKind::PermissionDenied => false,
            std::io::ErrorKind::NotFound => false,
            std::io::ErrorKind::AlreadyExists => false,
            _ => true, // Default to retryable for other IO errors
        }
    }
}

/// Implementation for serde_json::Error
impl RetryableError for serde_json::Error {
    fn is_retryable(&self) -> bool {
        // JSON parsing errors are typically not retryable
        false
    }
}

/// Helper macro for using ? operator with ActivityResult
/// This macro converts Result<T, E> to ActivityResult automatically
#[macro_export]
macro_rules! try_activity {
    ($expr:expr) => {
        match $expr {
            Ok(val) => val,
            Err(e) => return $crate::ActivityResult::from(Err(e)),
        }
    };
}

#[cfg(test)]
mod example_handlers {
    use super::*;
    use crate::activity::activity::{ActivityContext, ActivityHandler, ActivityResult};
    use async_trait::async_trait;
    use serde_json::json;

    /// Example handler demonstrating ? operator usage pattern
    pub struct FileProcessingActivity;

    #[async_trait]
    impl ActivityHandler for FileProcessingActivity {
        async fn handle(
            &self,
            payload: serde_json::Value,
            _context: ActivityContext,
        ) -> ActivityResult {
            // Internal function that returns Result, then convert to ActivityResult
            async fn process_file(
                payload: serde_json::Value,
            ) -> Result<serde_json::Value, ActivityError> {
                // Extract file path with ? operator
                let file_path = payload["file_path"]
                    .as_str()
                    .ok_or(ActivityError::NonRetry(
                        "Missing file_path in payload".to_string(),
                    ))?;

                // Validate input
                if file_path.is_empty() {
                    return Err(ActivityError::NonRetry("Empty file path".to_string()));
                }

                // Read file content - convert IO error to ActivityError
                let content = std::fs::read_to_string(file_path).map_err(|e| {
                    if e.is_retryable() {
                        ActivityError::Retry(e.to_string())
                    } else {
                        ActivityError::NonRetry(e.to_string())
                    }
                })?;

                // Parse as JSON - convert serde error to ActivityError
                let json_content: serde_json::Value = serde_json::from_str(&content)
                    .map_err(|e| ActivityError::NonRetry(e.to_string()))?;

                // Process the data
                Ok(json!({
                    "original_file": file_path,
                    "content_length": content.len(),
                    "json_keys": json_content.as_object()
                        .map(|obj| obj.keys().collect::<Vec<_>>())
                        .unwrap_or_default(),
                    "processed_at": chrono::Utc::now().to_rfc3339()
                }))
            }

            // Convert Result to ActivityResult
            match process_file(payload).await {
                Ok(data) => ActivityResult::Success(Some(data)),
                Err(ActivityError::Retry(msg)) => ActivityResult::Retry(msg),
                Err(ActivityError::NonRetry(msg)) => ActivityResult::NonRetry(msg),
            }
        }

        fn activity_type(&self) -> String {
            "file_processing_example".to_string()
        }
    }

    /// Example showing how to use From<Result<T, E>> conversion
    pub struct NetworkActivity;

    #[async_trait]
    impl ActivityHandler for NetworkActivity {
        async fn handle(
            &self,
            payload: serde_json::Value,
            _context: ActivityContext,
        ) -> ActivityResult {
            // Helper function that returns Result
            fn extract_url(payload: &serde_json::Value) -> Result<String, ActivityError> {
                payload["url"]
                    .as_str()
                    .ok_or(ActivityError::NonRetry("Missing URL".to_string()))
                    .map(|s| s.to_string())
            }

            // Use the From implementation to convert Result to ActivityResult
            let url = match extract_url(&payload) {
                Ok(url) => url,
                Err(e) => return ActivityResult::from(Err::<serde_json::Value, _>(e)),
            };

            // Simulate network operation
            let result = simulate_network_call(&url);

            // Convert Result to ActivityResult using From trait
            ActivityResult::from(result.map(|res| {
                json!({
                    "url": url,
                    "result": res,
                    "status": "completed"
                })
            }))
        }

        fn activity_type(&self) -> String {
            "network_example".to_string()
        }
    }

    // Simulate a network call that returns Result
    fn simulate_network_call(url: &str) -> Result<String, NetworkError> {
        if url.contains("error") {
            Err(NetworkError::ConnectionFailed(
                "Simulated connection error".to_string(),
            ))
        } else if url.contains("timeout") {
            Err(NetworkError::Timeout)
        } else {
            Ok(format!("Response from {}", url))
        }
    }

    // Custom error type with RetryableError implementation
    #[derive(Debug)]
    enum NetworkError {
        ConnectionFailed(String),
        Timeout,
        InvalidUrl(String),
    }

    impl std::fmt::Display for NetworkError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                NetworkError::ConnectionFailed(msg) => write!(f, "Connection failed: {}", msg),
                NetworkError::Timeout => write!(f, "Network timeout"),
                NetworkError::InvalidUrl(url) => write!(f, "Invalid URL: {}", url),
            }
        }
    }

    impl std::error::Error for NetworkError {}

    impl RetryableError for NetworkError {
        fn is_retryable(&self) -> bool {
            match self {
                NetworkError::ConnectionFailed(_) => true, // Retry connection failures
                NetworkError::Timeout => true,             // Retry timeouts
                NetworkError::InvalidUrl(_) => false,      // Don't retry invalid URLs
            }
        }
    }
}
