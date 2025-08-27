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

impl From<serde_json::Error> for ActivityError {
    fn from(error: serde_json::Error) -> Self {
        if error.is_retryable() {
            ActivityError::Retry(format!("JSON error: {}", error))
        } else {
            ActivityError::NonRetry(format!("JSON error: {}", error))
        }
    }
}

impl From<std::io::Error> for ActivityError {
    fn from(error: std::io::Error) -> Self {
        if error.is_retryable() {
            ActivityError::Retry(format!("IO error: {}", error))
        } else {
            ActivityError::NonRetry(format!("IO error: {}", error))
        }
    }
}

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
    use crate::activity::activity::{ActivityContext, ActivityHandler};
    use async_trait::async_trait;
    use serde_json::json;
    use crate::ActivityHandlerResult;

    /// Example handler demonstrating ? operator usage pattern
    pub struct FileProcessingActivity;

    #[async_trait]
    impl ActivityHandler for FileProcessingActivity {
        async fn handle(
            &self,
            payload: serde_json::Value,
            _context: ActivityContext,
        ) -> ActivityHandlerResult {
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
                Ok(Some(json!({
                    "original_file": file_path,
                    "content_length": content.len(),
                    "json_keys": json_content.as_object()
                        .map(|obj| obj.keys().collect::<Vec<_>>())
                        .unwrap_or_default(),
                    "processed_at": chrono::Utc::now().to_rfc3339()
                })))
        }

        fn activity_type(&self) -> String {
            "file_processing_example".to_string()
        }
    }
}
