use async_trait::async_trait;
use runner_q::{ActivityContext, ActivityError, ActivityHandler, ActivityResult, RetryableError};
use serde_json::json;

/// Example showing three different patterns for using ActivityResult with ? operator compatibility
pub struct EmailSendingActivity;

#[async_trait]
impl ActivityHandler for EmailSendingActivity {
    async fn handle(
        &self,
        payload: serde_json::Value,
        _context: ActivityContext,
    ) -> ActivityResult {
        // Pattern 1: Direct conversion using From trait
        let email_data = match parse_email_payload(&payload) {
            Ok(data) => data,
            Err(e) => return ActivityResult::from(Err::<serde_json::Value, _>(e)),
        };

        // Pattern 2: Manual error handling with early return
        let smtp_config = match payload["smtp_config"].as_object() {
            Some(config) => config,
            None => return ActivityResult::NonRetry("Missing SMTP configuration".to_string()),
        };

        // Pattern 3: Using Result internally, then convert to ActivityResult
        match send_email_internal(email_data, smtp_config).await {
            Ok(message_id) => ActivityResult::Success(Some(json!({
                "message_id": message_id,
                "status": "sent",
                "timestamp": chrono::Utc::now().to_rfc3339()
            }))),
            Err(e) => ActivityResult::from(Err::<serde_json::Value, _>(e)),
        }
    }

    fn activity_type(&self) -> String {
        "send_email".to_string()
    }
}

// Helper function that returns Result<T, ActivityError> - can use ? operator
fn parse_email_payload(payload: &serde_json::Value) -> Result<EmailData, ActivityError> {
    let to = payload["to"]
        .as_str()
        .ok_or(ActivityError::NonRetry("Missing 'to' field".to_string()))?;

    let subject = payload["subject"].as_str().ok_or(ActivityError::NonRetry(
        "Missing 'subject' field".to_string(),
    ))?;

    let body = payload["body"]
        .as_str()
        .ok_or(ActivityError::NonRetry("Missing 'body' field".to_string()))?;

    // Validate email format (simplified)
    if !to.contains('@') {
        return Err(ActivityError::NonRetry("Invalid email format".to_string()));
    }

    Ok(EmailData {
        to: to.to_string(),
        subject: subject.to_string(),
        body: body.to_string(),
    })
}

#[derive(Debug)]
struct EmailData {
    to: String,
    subject: String,
    body: String,
}

// Simulate sending email
async fn send_email_internal(
    email_data: EmailData,
    _smtp_config: &serde_json::Map<String, serde_json::Value>,
) -> Result<String, EmailError> {
    // Simulate network call that might fail
    if email_data.to.contains("invalid") {
        Err(EmailError::InvalidRecipient(email_data.to))
    } else if email_data.to.contains("timeout") {
        Err(EmailError::SmtpTimeout)
    } else {
        // Success case
        Ok(format!("msg_{}", uuid::Uuid::new_v4()))
    }
}

// Custom error type for email operations
#[derive(Debug)]
enum EmailError {
    SmtpTimeout,
    SmtpConnectionFailed(String),
    InvalidRecipient(String),
    AuthenticationFailed,
}

impl std::fmt::Display for EmailError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EmailError::SmtpTimeout => write!(f, "SMTP server timeout"),
            EmailError::SmtpConnectionFailed(msg) => write!(f, "SMTP connection failed: {}", msg),
            EmailError::InvalidRecipient(email) => write!(f, "Invalid recipient: {}", email),
            EmailError::AuthenticationFailed => write!(f, "SMTP authentication failed"),
        }
    }
}

impl std::error::Error for EmailError {}

impl RetryableError for EmailError {
    fn is_retryable(&self) -> bool {
        match self {
            EmailError::SmtpTimeout => true,
            EmailError::SmtpConnectionFailed(_) => true,
            EmailError::InvalidRecipient(_) => false, // Invalid email shouldn't be retried
            EmailError::AuthenticationFailed => false, // Auth failure shouldn't be retried
        }
    }
}

// Implement From<Result<T, ActivityError>> for ActivityResult (already implemented in the library)
// This allows us to use ? operator with ActivityError directly

fn main() {
    println!("This example demonstrates ActivityResult ? operator compatibility patterns:");
    println!("1. Direct conversion using From trait");
    println!("2. Manual error handling with early return");
    println!("3. Using Result internally, then convert to ActivityResult");
    println!("4. Custom error types with RetryableError trait");
}
