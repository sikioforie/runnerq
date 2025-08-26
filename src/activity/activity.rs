use async_trait::async_trait;

use crate::queue::queue::ActivityQueueTrait;
use crate::runner::runner::ActivityExecutor;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

// Import error handling types
use super::error::{ActivityError, RetryableError};

/// Activity priority levels
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum ActivityPriority {
    Low = 1,
    Normal = 2,
    High = 3,
    Critical = 4,
}

impl Default for ActivityPriority {
    fn default() -> Self {
        Self::Normal
    }
}

/// Activity status tracking
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ActivityStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Retrying,
    DeadLetter,
}

pub struct ActivityOption {
    pub priority: Option<ActivityPriority>,
    pub max_retries: u32,
    pub timeout_seconds: u64,
}

/// Represents an Activity to be processed
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Activity {
    pub id: Uuid,
    pub activity_type: String,
    pub payload: serde_json::Value,
    pub priority: ActivityPriority,
    pub status: ActivityStatus,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub scheduled_at: Option<chrono::DateTime<chrono::Utc>>,
    pub retry_count: u32,
    pub max_retries: u32,
    pub timeout_seconds: u64,
    pub retry_delay_seconds: u64,
    pub metadata: HashMap<String, String>,
}

impl Activity {
    pub fn new(
        activity_type: String,
        payload: serde_json::Value,
        option: Option<ActivityOption>,
    ) -> Self {
        let (priority, max_retries, timeout_seconds) = if let Some(opt) = option {
            (
                opt.priority.unwrap_or(ActivityPriority::default()),
                opt.max_retries,
                opt.timeout_seconds,
            )
        } else {
            (ActivityPriority::default(), 3, 300)
        };

        Self {
            id: Uuid::new_v4(),
            activity_type,
            payload,
            priority,
            status: ActivityStatus::Pending,
            created_at: chrono::Utc::now(),
            scheduled_at: None,
            retry_count: 0,
            max_retries,
            timeout_seconds,
            retry_delay_seconds: 1,
            metadata: HashMap::new(),
        }
    }
}

/// Context provided to Activity handlers during execution
#[derive(Clone)]
pub struct ActivityContext {
    pub activity_id: Uuid,
    pub activity_type: String,
    pub retry_count: u32,
    pub metadata: HashMap<String, String>,
    pub worker_engine: Arc<dyn ActivityExecutor>,
}

/// Result of Activity execution
#[derive(Debug)]
pub enum ActivityResult {
    Success(Option<serde_json::Value>),
    Retry(String),
    NonRetry(String),
}

impl ActivityResult {
    /// Create a retry result with a message
    pub fn retry<S: Into<String>>(msg: S) -> Self {
        ActivityResult::Retry(msg.into())
    }

    /// Create a non-retry result with a message
    pub fn non_retry<S: Into<String>>(msg: S) -> Self {
        ActivityResult::NonRetry(msg.into())
    }

    /// Create a success result with optional data
    pub fn success<T: Into<serde_json::Value>>(data: Option<T>) -> Self {
        ActivityResult::Success(data.map(|d| d.into()))
    }

    /// Convert a Result to ActivityResult
    pub fn from_result<T, E>(result: Result<T, E>) -> Self
    where
        T: Into<serde_json::Value>,
        E: std::fmt::Display + RetryableError,
    {
        result.into()
    }
}

/// Convert from Result<T, ActivityError> to ActivityResult
impl<T> From<Result<T, ActivityError>> for ActivityResult
where
    T: Into<serde_json::Value>,
{
    fn from(result: Result<T, ActivityError>) -> Self {
        match result {
            Ok(value) => ActivityResult::Success(Some(value.into())),
            Err(ActivityError::Retry(msg)) => ActivityResult::Retry(msg),
            Err(ActivityError::NonRetry(msg)) => ActivityResult::NonRetry(msg),
        }
    }
}

/// Convert from Result<T, E> to ActivityResult where E implements RetryableError
impl<T, E> From<Result<T, E>> for ActivityResult
where
    T: Into<serde_json::Value>,
    E: std::fmt::Display + RetryableError,
{
    fn from(result: Result<T, E>) -> Self {
        match result {
            Ok(value) => ActivityResult::Success(Some(value.into())),
            Err(err) => {
                if err.is_retryable() {
                    ActivityResult::Retry(err.to_string())
                } else {
                    ActivityResult::NonRetry(err.to_string())
                }
            }
        }
    }
}

/// Trait that all Activity handlers must implement
#[async_trait]
pub trait ActivityHandler: Send + Sync {
    async fn handle(&self, payload: serde_json::Value, context: ActivityContext) -> ActivityResult;

    fn activity_type(&self) -> String;
}

/// Registry for Activity handlers
pub(crate) type ActivityHandlerRegistry = HashMap<String, Arc<dyn ActivityHandler>>;

pub struct ActivityFuture {
    queue: Arc<dyn ActivityQueueTrait>,
    activity_id: Uuid,
}

impl ActivityFuture {
    pub(crate) fn new(queue: Arc<dyn ActivityQueueTrait>, activity_id: Uuid) -> Self {
        Self { queue, activity_id }
    }

    // this method should consume the instance and hence only let get_result to be callable once on an ActivityFuture
    pub async fn get_result(self) -> Result<serde_json::Value, crate::WorkerError> {
        // Poll for result with timeout
        let timeout = std::time::Duration::from_secs(300); // 5 minutes timeout
        let start_time = std::time::Instant::now();

        loop {
            if let Some(result) = self.queue.get_result(self.activity_id).await? {
                return Ok(result);
            }

            if start_time.elapsed() > timeout {
                return Err(crate::WorkerError::Timeout);
            }

            // Wait a bit before polling again
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    }
}
