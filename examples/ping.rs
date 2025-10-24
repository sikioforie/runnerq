use runner_q::{ActivityQueue, WorkerEngine, ActivityPriority, ActivityOption};
use runner_q::{ActivityHandler, ActivityContext, ActivityHandlerResult, ActivityError};
use runner_q::config::WorkerConfig;
use std::sync::Arc;
use async_trait::async_trait;
use serde::{Serialize, Deserialize};
use serde_json::Value;
use tracing::info;


// Implement activity handler
pub struct PingActivity;

#[async_trait]
impl ActivityHandler for PingActivity {
    async fn handle(&self, payload: serde_json::Value, context: ActivityContext) -> ActivityHandlerResult {        
        let input:PingInput = serde_json::from_value(payload)
            .map_err(|_| ActivityError::NonRetry("Invalid payload format".to_string()))?;

        Ok(Some(
            serde_json::to_value(Pong{operation_id: input.operation_id})
                .map_err(|_| ActivityError::NonRetry("Invalid pong format".to_string()))?
        ))
    }

    fn activity_type(&self) -> String {
        "ping".to_string()
    }
}


#[derive(Debug, Serialize, Deserialize)]
pub struct PingInput {
    operation_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Pong {
    operation_id: String,
}



#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().init();

    // Create Redis connection pool
    let redis_pool = bb8_redis::bb8::Pool::builder()
        .build(bb8_redis::RedisConnectionManager::new("redis://127.0.0.1:6379")?)
        .await?;

    // Create worker engine
    let config = WorkerConfig::default();
    let mut worker_engine = WorkerEngine::new(redis_pool, config);

    // Register activity handler
    worker_engine.register_activity("ping".to_string(), Arc::new(PingActivity));

    // Execute an activity with custom options
    let future = worker_engine.execute_activity(
        "ping".to_string(),
        serde_json::to_value(Pong{operation_id: "0x1".into()}).unwrap(),
        Some(ActivityOption {
            priority: Some(ActivityPriority::High),
            max_retries: 5,
            timeout_seconds: 600, // 10 minutes
            scheduled_at: None, // Execute immediately
        })
    ).await?;

    // Spawn a task to handle the result
    tokio::spawn(async move {
        if let Ok(result) = future.get_result().await {
            match result {
                None => {}
                Some(data) => {
                    let pong: Pong = serde_json::from_value(data).unwrap();
                    info!("pong::{:?}", pong.operation_id);
                }
            }
        }
    });

    // Start the worker engine (this will run indefinitely)
    worker_engine.start().await?;

    Ok(())
}
