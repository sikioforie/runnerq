use std::sync::Arc;
use std::sync::{Mutex, RwLock, OnceLock};
use crate::WorkerError;
use std::sync::atomic::{AtomicBool, Ordering};
use std::collections::HashMap;

static POOLED_ACTIVITIES: OnceLock<Mutex<Vec<String>>> = OnceLock::new();
   
/// Check if an activity is acquired by a worker
/// if it's not, then it's then recorded as aquired
///
pub fn is_acquired(activity_id: &str) -> Result<bool, WorkerError>  {
    if let Some(pooled) = POOLED_ACTIVITIES.get() {
        match pooled.lock(){
            Ok(mut pooled) => {
                if pooled.iter().any(|id| id == activity_id){
                    return Ok(true);
                }
                
                pooled.push(activity_id.to_string());
                return Ok(false);
            },
            Err(e) => {
                return Err(WorkerError::CustomError(e.to_string()));
            } 
        }
    } else {
        POOLED_ACTIVITIES.get_or_init(|| Mutex::new(Vec::new()));
        is_acquired(activity_id)
    }
}



#[test]
fn test_is_acquired(){
    let acquired = is_acquired("0x1").unwrap();
    assert!(!acquired);

    let acquired = is_acquired("0x1").unwrap();
    assert!(acquired);    
}

// -------------------------------------------------------

use crate::activity::activity::Activity;

static RUNNING_ACTIVITIES_STORE: OnceLock<Mutex<HashMap<String, Activity>>> = OnceLock::new();

/// Move an activity to running store
pub fn activity_move_to_running_store(activity: &Activity) -> Result<(), WorkerError>  {
    if let Some(store) = RUNNING_ACTIVITIES_STORE.get() {
        match store.lock(){
            Ok(mut store) => {               
                store.insert(activity_id.to_string(), activity.clone());
                return Ok(());
            },
            Err(e) => {
                return Err(WorkerError::CustomError(e.to_string()));
            } 
        }
    } else {
        POOLED_ACTIVITIES.get_or_init(|| Mutex::new(Vec::new()));
        activity_move_to_running_store(activity)
    }
}

// ----------------------------------------------------------
static COMPLETITED_ACTIVITIES_STORE: OnceLock<Mutex<HashMap<String, Activity>>> = OnceLock::new();

/// Move an activity to completed store
pub fn activity_move_to_completed_store(activity: &Activity) -> Result<(), WorkerError>  {
    if let Some(store) = RUNNING_ACTIVITIES_STORE.get() {
        match store.lock(){
            Ok(mut store) => {               
                store.insert(activity_id.to_string(), activity.clone());
                return Ok(());
            },
            Err(e) => {
                return Err(WorkerError::CustomError(e.to_string()));
            } 
        }
    } else {
        POOLED_ACTIVITIES.get_or_init(|| Mutex::new(Vec::new()));
        activity_move_to_running_store(activity)
    }
}
