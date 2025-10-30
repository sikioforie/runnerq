pub struct NetworkInfo {
    pub ip: IpAddr
}

impl NetworkInfo {
    pub async fn acquire() -> Result<Self, WorkerError> {
        Ok(Self  {
            ip: toolbox::ip::get_public_ip(None).map_err(|e| WorkerError::CustomError(e.to_string()))?
        })
    }
}

#[tokio::test]
async fn test_acquire_network_info(){
    let net_info = NetworkInfo::acquire().await;
    assert!(net_info.is_ok());
}


use crate::WorkerError;
use std::net::IpAddr;
