//! Ephemeral ownership only. Losing these keys never deletes or settles durable work.
use crate::{
    delivery::DeliveryError,
    identity::{Action, authorize},
    protocol::{PRESENCE_EXPIRY_SECONDS, Principal},
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone)]
pub struct Presence {
    connection: redis::aio::MultiplexedConnection,
    namespace: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Owner {
    pub gateway_id: Uuid,
    pub session_id: Uuid,
}

pub(crate) fn agent_scope(agent: &Principal) -> Result<(Uuid, Uuid, Uuid), DeliveryError> {
    if let Principal::Agent {
        workspace_id,
        app_id,
        agent_id,
        ..
    } = agent
    {
        authorize(agent, *workspace_id, Action::Deliver { app_id: *app_id })
            .map_err(|_| DeliveryError::Forbidden)?;
        Ok((*workspace_id, *app_id, *agent_id))
    } else {
        Err(DeliveryError::Forbidden)
    }
}
impl Presence {
    pub fn new(
        connection: redis::aio::MultiplexedConnection,
        namespace: &str,
    ) -> Result<Self, DeliveryError> {
        if namespace.is_empty()
            || namespace.len() > 128
            || !namespace
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || c == b'_' || c == b'-')
        {
            return Err(DeliveryError::InvalidInput);
        }
        Ok(Self {
            connection,
            namespace: namespace.to_owned(),
        })
    }
    pub fn subject(&self, gateway: Uuid) -> String {
        format!("{}.v1.gateways.{gateway}.deliveries", self.namespace)
    }
    pub(crate) fn delivery_subject(&self, workspace: Uuid, app: Uuid) -> String {
        format!(
            "{}.v1.workspaces.{workspace}.apps.{app}.deliveries",
            self.namespace
        )
    }
    fn index(&self, workspace: Uuid, app: Uuid) -> String {
        format!("{}:v1:presence:{workspace}:{app}", self.namespace)
    }
    fn key(&self, workspace: Uuid, app: Uuid, agent: Uuid) -> String {
        format!("{}:{agent}", self.index(workspace, app))
    }

    pub async fn register(&self, agent: &Principal, owner: &Owner) -> Result<(), DeliveryError> {
        let (workspace, app, id) = agent_scope(agent)?;
        let value = serde_json::to_string(owner).map_err(|_| DeliveryError::Internal)?;
        redis::cmd("EVAL").arg("local t=redis.call('TIME'); redis.call('SET',KEYS[1],ARGV[1],'EX',ARGV[2]); redis.call('ZREMRANGEBYSCORE',KEYS[2],'-inf',t[1]); redis.call('ZADD',KEYS[2],t[1]+ARGV[2],KEYS[1]); redis.call('EXPIRE',KEYS[2],ARGV[2]); return 1")
            .arg(2).arg(self.key(workspace,app,id)).arg(self.index(workspace,app)).arg(value).arg(PRESENCE_EXPIRY_SECONDS)
            .query_async::<i64>(&mut self.connection.clone()).await.map_err(|_|DeliveryError::Unavailable)?;
        Ok(())
    }
    pub async fn owner(&self, agent: &Principal) -> Result<Option<Owner>, DeliveryError> {
        let (workspace, app, id) = agent_scope(agent)?;
        let value: Option<String> = redis::cmd("GET")
            .arg(self.key(workspace, app, id))
            .query_async(&mut self.connection.clone())
            .await
            .map_err(|_| DeliveryError::Unavailable)?;
        value
            .map(|s| serde_json::from_str(&s).map_err(|_| DeliveryError::Unavailable))
            .transpose()
    }
    pub async fn heartbeat(&self, agent: &Principal, owner: &Owner) -> Result<(), DeliveryError> {
        let (workspace, app, id) = agent_scope(agent)?;
        let value = serde_json::to_string(owner).map_err(|_| DeliveryError::Internal)?;
        let changed:i64=redis::cmd("EVAL").arg("if redis.call('GET',KEYS[1])~=ARGV[1] then return 0 end; local t=redis.call('TIME'); redis.call('EXPIRE',KEYS[1],ARGV[2]); redis.call('ZADD',KEYS[2],t[1]+ARGV[2],KEYS[1]); redis.call('EXPIRE',KEYS[2],ARGV[2]); return 1")
            .arg(2).arg(self.key(workspace,app,id)).arg(self.index(workspace,app)).arg(value).arg(PRESENCE_EXPIRY_SECONDS)
            .query_async(&mut self.connection.clone()).await.map_err(|_|DeliveryError::Unavailable)?;
        if changed == 1 {
            Ok(())
        } else {
            Err(DeliveryError::Conflict)
        }
    }
    pub async fn remove(&self, agent: &Principal, owner: &Owner) -> Result<(), DeliveryError> {
        let (workspace, app, id) = agent_scope(agent)?;
        redis::cmd("EVAL").arg("if redis.call('GET',KEYS[1])==ARGV[1] then redis.call('DEL',KEYS[1]); redis.call('ZREM',KEYS[2],KEYS[1]); end; return 1")
            .arg(2).arg(self.key(workspace,app,id)).arg(self.index(workspace,app)).arg(serde_json::to_string(owner).map_err(|_|DeliveryError::Internal)?)
            .query_async::<i64>(&mut self.connection.clone()).await.map_err(|_|DeliveryError::Unavailable)?;
        Ok(())
    }
    pub(crate) async fn owners(
        &self,
        workspace: Uuid,
        app: Uuid,
    ) -> Result<Vec<Owner>, DeliveryError> {
        let values:Vec<String>=redis::cmd("EVAL").arg("local t=redis.call('TIME'); redis.call('ZREMRANGEBYSCORE',KEYS[1],'-inf',t[1]); local keys=redis.call('ZRANGE',KEYS[1],0,-1); local result={}; for _,key in ipairs(keys) do local v=redis.call('GET',key); if v then table.insert(result,v) end end; return result")
            .arg(1).arg(self.index(workspace,app)).query_async(&mut self.connection.clone()).await.map_err(|_|DeliveryError::Unavailable)?;
        values
            .into_iter()
            .map(|v| serde_json::from_str(&v).map_err(|_| DeliveryError::Unavailable))
            .collect()
    }
}
