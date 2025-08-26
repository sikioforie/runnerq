//! Runner-Q: A Redis-based activity queue and worker system for Rust
//!
//! This crate provides a robust, scalable activity queue system built on Redis with support for:
//! - Priority-based activity processing
//! - Activity scheduling and retries
//! - Dead letter queue handling
//! - Concurrent activity processing
//! - Graceful shutdown handling
//!
//! # Example
//!
//! ```rust,no_run
//! use runner_q::{ActivityQueue, WorkerEngine, ActivityPriority, ActivityOption};
//! use runner_q::{ActivityHandler, ActivityContext, ActivityResult};
//! use runner_q::config::WorkerConfig;
//! use std::sync::Arc;
//! use async_trait::async_trait;
//! use serde::{Serialize, Deserialize};
//!
//! // Define activity types
//! #[derive(Debug, Clone)]
//! enum MyActivityType {
//!     SendEmail,
//!     ProcessPayment,
//! }
//!
//!
//! impl std::fmt::Display for MyActivityType {
//!     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//!         match self {
//!             MyActivityType::SendEmail => write!(f, "send_email"),
//!             MyActivityType::ProcessPayment => write!(f, "process_payment"),
//!         }
//!     }
//! }
//!
//! // Implement activity handler
//! pub struct SendEmailActivity;
//!
//! #[async_trait]
//! impl ActivityHandler for SendEmailActivity {
//!     async fn handle(&self, payload: serde_json::Value, context: ActivityContext) -> ActivityResult {
//!         // Parse the email data
//!         let email_data: serde_json::Value = payload;
//!         let to = email_data["to"].as_str().unwrap_or("unknown");
//!         
//!         // Simulate sending email
//!         println!("Sending email to: {}", to);
//!         
//!         // Return success with result
//!         ActivityResult::Success(Some(serde_json::json!({
//!             "message": format!("Email sent to {}", to),
//!             "status": "delivered"
//!         })))
//!     }
//!
//!     fn activity_type(&self) -> String {
//!         MyActivityType::SendEmail.to_string()
//!     }
//! }
//!
//! #[derive(Debug, Serialize, Deserialize)]
//! pub struct EmailResult {
//!     message: String,
//!     status: String,
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Create Redis connection pool
//!     let redis_pool = bb8_redis::bb8::Pool::builder()
//!         .build(bb8_redis::RedisConnectionManager::new("redis://127.0.0.1:6379")?)
//!         .await?;
//!
//!     // Create worker engine
//!     let config = WorkerConfig::default();
//!     let mut worker_engine = WorkerEngine::new(redis_pool, config);
//!
//!     // Register activity handler
//!     let send_email_activity = SendEmailActivity;
//!     worker_engine.register_activity(MyActivityType::SendEmail.to_string(), Arc::new(send_email_activity));
//!
//!     // Execute an activity with custom options
//!     let future = worker_engine.execute_activity(
//!         MyActivityType::SendEmail.to_string(),
//!         serde_json::json!({"to": "user@example.com", "subject": "Welcome!"}),
//!         Some(ActivityOption {
//!             priority: Some(ActivityPriority::High),
//!             max_retries: 5,
//!             timeout_seconds: 600, // 10 minutes
//!         })
//!     ).await?;
//!
//!     // Execute an activity with default options
//!     let future2 = worker_engine.execute_activity(
//!         MyActivityType::SendEmail.to_string(),
//!         serde_json::json!({"to": "admin@example.com"}),
//!         None // Uses default priority (Normal), 3 retries, 300s timeout
//!     ).await?;
//!
//!     // Spawn a task to handle the result
//!     tokio::spawn(async move {
//!         if let Ok(result) = future.get_result().await {
//!             let email_result: EmailResult = serde_json::from_value(result).unwrap();
//!             println!("Email result: {:?}", email_result);
//!         }
//!     });
//!
//!     // Start the worker engine (this will run indefinitely)
//!     worker_engine.start().await?;
//!
//!     Ok(())
//! }
//! ```

pub mod activity;
pub mod config;
pub mod queue;
pub mod runner;
pub mod worker;

// Re-export main types for easy access
pub use crate::config::WorkerConfig;
pub use crate::queue::queue::{ActivityQueue, QueueStats};
pub use crate::runner::error::WorkerError;
pub use crate::runner::runner::{ActivityExecutor, WorkerEngine};
pub use activity::activity::{
    ActivityContext, ActivityFuture, ActivityHandler, ActivityOption, ActivityPriority,
    ActivityResult, ActivityStatus,
};
pub use activity::error::{ActivityError, RetryableError};
