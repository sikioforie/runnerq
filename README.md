# Runner-Q

A robust, scalable Redis-based activity queue and worker system for Rust applications.

## Features

- **Priority-based activity processing** - Support for Critical, High, Normal, and Low priority levels
- **Activity scheduling and retries** - Built-in retry mechanism with exponential backoff
- **Dead letter queue** - Failed activities are moved to a dead letter queue for inspection
- **Concurrent activity processing** - Configurable number of concurrent workers
- **Graceful shutdown** - Proper shutdown handling with signal support
- **Activity timeouts** - Configurable timeout per activity type
- **Activity metadata** - Support for custom metadata on activities
- **Redis persistence** - Activities are stored in Redis for durability

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
runner_q = "0.1.0"
```

## Quick Start

```rust
use runner_q::{ActivityQueue, WorkerEngine, ActivityPriority, ActivityType, ActivityOption};
use runner_q::{ActivityHandler, ActivityContext, ActivityResult};
use runner_q::config::WorkerConfig;
use std::sync::Arc;
use async_trait::async_trait;
use serde::{Serialize, Deserialize};

// Define activity types
#[derive(Debug, Clone)]
enum MyActivityType {
    SendEmail,
    ProcessPayment,
}

impl std::fmt::Display for MyActivityType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MyActivityType::SendEmail => write!(f, "send_email"),
            MyActivityType::ProcessPayment => write!(f, "process_payment"),
        }
    }
}

// Implement activity handler
pub struct SendEmailActivity;

#[async_trait]
impl ActivityHandler for SendEmailActivity {
    async fn handle(&self, payload: serde_json::Value, context: ActivityContext) -> ActivityResult {
        // Process the email activity
        let to = payload["to"].as_str().unwrap_or("unknown");
        println!("Sending email to: {}", to);
        
        ActivityResult::Success(Some(serde_json::json!({
            "message": format!("Email sent to {}", to),
            "status": "delivered"
        })))
    }

    fn activity_type(&self) -> String {
        MyActivityType::SendEmail.to_string()
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create Redis connection pool
    let redis_pool = bb8_redis::bb8::Pool::builder()
        .build(bb8_redis::RedisConnectionManager::new("redis://127.0.0.1:6379")?)
        .await?;

    // Create worker engine
    let config = WorkerConfig::default();
    let mut worker_engine = WorkerEngine::new(redis_pool, config);

    // Register activity handler
    let send_email_activity = SendEmailActivity;
    worker_engine.register_activity(MyActivityType::SendEmail, Arc::new(send_email_activity));

    // Execute an activity with custom options
    let future = worker_engine.execute_activity(
        MyActivityType::SendEmail,
        serde_json::json!({"to": "user@example.com", "subject": "Welcome!"}),
        Some(ActivityOption {
            priority: Some(ActivityPriority::High),
            max_retries: 5,
            timeout_seconds: 600, // 10 minutes
        })
    ).await?;

    // Execute an activity with default options
    let future2 = worker_engine.execute_activity(
        MyActivityType::SendEmail,
        serde_json::json!({"to": "admin@example.com"}),
        None // Uses default priority (Normal), 3 retries, 300s timeout
    ).await?;

    // Spawn a task to handle the result
    tokio::spawn(async move {
        if let Ok(result) = future.get_result().await {
            println!("Email result: {:?}", result);
        }
    });

    // Start the worker engine (this will run indefinitely)
    worker_engine.start().await?;

    Ok(())
}
```

## Activity Types

Activity types in Runner-Q are defined using the `ActivityType` trait. Any type that implements `Display + Clone + Send + Sync + 'static` automatically implements this trait.

### Defining Activity Types

You can define custom activity types using enums:

```rust
#[derive(Debug, Clone)]
enum MyActivityType {
    SendEmail,
    ProcessPayment,
    ProvisionCard,
    UpdateCardStatus,
    ProcessWebhookEvent,
}

impl std::fmt::Display for MyActivityType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MyActivityType::SendEmail => write!(f, "send_email"),
            MyActivityType::ProcessPayment => write!(f, "process_payment"),
            MyActivityType::ProvisionCard => write!(f, "provision_card"),
            MyActivityType::UpdateCardStatus => write!(f, "update_card_status"),
            MyActivityType::ProcessWebhookEvent => write!(f, "process_webhook_event"),
        }
    }
}
```

### String-based Activity Types

You can also use simple string wrappers:

```rust
#[derive(Debug, Clone)]
struct StringActivityType(String);

impl std::fmt::Display for StringActivityType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

// Usage
let activity_type = StringActivityType("custom_activity".to_string());
```

## Configuration

```rust
use runner_q::config::WorkerConfig;

let config = WorkerConfig {
    queue_name: "my_queue".to_string(),
    max_concurrent_activities: 10,
    redis_url: "redis://127.0.0.1:6379".to_string(),
};
```

## Custom Activity Handlers

You can create custom activity handlers by implementing the `ActivityHandler` trait:

```rust
use runner_q::{ActivityContext, ActivityHandler, ActivityResult, ActivityType};
use async_trait::async_trait;
use serde_json::Value;

#[derive(Debug, Clone)]
enum MyActivityType {
    SendEmail,
    ProcessPayment,
}

impl std::fmt::Display for MyActivityType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MyActivityType::SendEmail => write!(f, "send_email"),
            MyActivityType::ProcessPayment => write!(f, "process_payment"),
        }
    }
}

pub struct PaymentActivity {
    // Add your dependencies here (database connections, external APIs, etc.)
}

#[async_trait]
impl ActivityHandler for PaymentActivity {
    async fn handle(&self, payload: Value, context: ActivityContext) -> ActivityResult {
        // Parse the payment data
        let amount = payload["amount"].as_f64().unwrap_or(0.0);
        let currency = payload["currency"].as_str().unwrap_or("USD");
        
        println!("Processing payment: {} {}", amount, currency);
        
        // Simulate payment processing
        if amount > 0.0 {
            // Return success with result data
            ActivityResult::Success(Some(serde_json::json!({
                "transaction_id": "txn_123456",
                "amount": amount,
                "currency": currency,
                "status": "completed"
            })))
        } else {
            // Return non-retryable failure
            ActivityResult::NonRetry("Invalid amount".to_string())
        }
    }

    fn activity_type(&self) -> String {
        MyActivityType::ProcessPayment.to_string()
    }
}

// Register the handler
worker_engine.register_activity(MyActivityType::ProcessPayment, Arc::new(PaymentActivity {}));
```

## Activity Priority and Options

Activities can be configured using the `ActivityOption` struct:

```rust
use runner_q::{ActivityPriority, ActivityOption};

// High priority with custom retry and timeout settings
let future = worker_engine.execute_activity(
    MyActivityType::SendEmail,
    serde_json::json!({"to": "user@example.com"}),
    Some(ActivityOption {
        priority: Some(ActivityPriority::Critical), // Highest priority
        max_retries: 10,                            // Retry up to 10 times
        timeout_seconds: 900,                       // 15 minute timeout
    })
).await?;

// Use default options (Normal priority, 3 retries, 300s timeout)
let future = worker_engine.execute_activity(
    MyActivityType::SendEmail,
    serde_json::json!({"to": "user@example.com"}),
    None
).await?;
```

Available priorities:
- `ActivityPriority::Critical` - Highest priority (processed first)
- `ActivityPriority::High` - High priority
- `ActivityPriority::Normal` - Default priority
- `ActivityPriority::Low` - Lowest priority

## Getting Activity Results

Activities can return results that can be retrieved asynchronously:

```rust
use serde::{Serialize, Deserialize};

#[derive(Debug, Serialize, Deserialize)]
struct EmailResult {
    message: String,
    status: String,
}

let future = worker_engine.execute_activity(
    MyActivityType::SendEmail,
    serde_json::json!({"to": "user@example.com"}),
    None
).await?;

// Get the result (this will wait until the activity completes)
let result_value = future.get_result().await?;
let email_result: EmailResult = serde_json::from_value(result_value)?;
println!("Email result: {:?}", email_result);
```

## Error Handling

The library provides comprehensive error handling:

### Activity Handler Results

In your activity handlers, you can return different result types:

```rust
#[async_trait]
impl ActivityHandler for MyActivity {
    async fn handle(&self, payload: serde_json::Value, context: ActivityContext) -> ActivityResult {
        match some_operation() {
            Ok(data) => {
                // Success with optional result data
                ActivityResult::Success(Some(serde_json::json!({"result": data})))
            }
            Err(retryable_error) => {
                // Will be retried with exponential backoff
                ActivityResult::Retry(format!("Temporary error: {}", retryable_error))
            }
            Err(permanent_error) => {
                // Will not be retried, goes to dead letter queue
                ActivityResult::NonRetry(format!("Permanent error: {}", permanent_error))
            }
        }
    }
}
```

### Worker Engine Errors

```rust
use runner_q::WorkerError;

match worker_engine.execute_activity(activity_type, payload, options).await {
    Ok(future) => {
        // Activity was successfully enqueued
        match future.get_result().await {
            Ok(result) => println!("Activity completed: {:?}", result),
            Err(WorkerError::Timeout) => println!("Activity timed out"),
            Err(e) => println!("Activity failed: {}", e),
        }
    }
    Err(e) => println!("Failed to enqueue activity: {}", e),
}
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.

