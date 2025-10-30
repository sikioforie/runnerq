/*

Tracing activity execution for client and nexted activities:
- In the event of nexted activities, how do we know which parent activity executed an activity with faulty inputs 
- This will help find where an activity was executed from be it another activity(through) or a client.
- We could introduce an address system that identifies both the client and which part of the system executes an activity;
- The address system may consisting of the client ip address and the path of the code executing an activity.
  - **Format:**  <ip address>/<type>:<path of code>
  - **Example:** `127.0.0.1:8080/fn:main`, `192.168.0.23:8080/activity:process_payment`
  - **Wild Idea:** Maybe the address could just caption the code path and line for precision. Eg. `127.0.0.1:8080::examples/ping.rs:68:13`

*/


#[derive(Debug, Clone)]
struct TraceExecutionData {
    pub network_info: NetworkInfo, 
    // pub code_location: CodeLocation,
}


 #[macro_export]
 macro_rules! execute {
    ($activity_type:expr, $payload:expr, $engine:&WorkerEngine) => {{
        // create_executor(
        //     $activity_type,
        //     $payload,
        //     $engine,
        //     format!("{}:{}:{}", file!(), line!(), column!())
        // )

        get_activity_executor($engine)
            .activity(activity_type)
            .payload(payload)
            .execute().await
    }}; 
}



 
fn get_activity_executor2(engine:&WorkerEngine) -> impl ActivityExecutor {
    engine.get_activity_executor2()
}

// // fn create_executor(activity_type: &str, payload: serde_json::Value, engine: &WorkerEngine, caller_location: CodeLocation) -> ActivityBuilder<'_>
// pub async fn create_executor(activity_type: &str, payload: serde_json::Value, engine: &WorkerEngine, caller_location: String) -> Result<ActivityFuture, WorkerError>{
//     engine
//         .get_activity_executor2()
//         .activity(activity_type)
//         .payload(payload)
//         .execute().await
// }

use crate::activity::activity::{ActivityFuture, ActivityHandlerRegistry, ActivityOption, };
use crate::{NetworkInfo, WorkerEngine, ActivityBuilder, ActivityExecutor};
use crate::runner::error::WorkerError;
use std::sync::Arc;
