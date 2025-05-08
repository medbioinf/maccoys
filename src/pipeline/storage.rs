use std::{
    collections::HashMap,
    str::FromStr,
    sync::{
        atomic::{AtomicU64, Ordering},
        RwLock,
    },
};

use dihardts_omicstools::proteomics::post_translational_modifications::PostTranslationalModification;
use futures::Future;
use rustis::commands::{GenericCommands, StringCommands};

use crate::io::comet::configuration::Configuration as CometConfiguration;

use super::{
    configuration::{PipelineStorageConfiguration, SearchParameters},
    errors::storage_error::{LocalStorageError, RedisStorageError, StorageError},
};

const TOTAL_SPECTRUM_COUNT_PREFIX: &str = "total_spectrum_count_";
const FINISHED_SPECTRUM_COUNT_PREFIX: &str = "finished_spectrum_count_";

/// Central storage for configuration, PTM etc.
///
pub trait PipelineStorage: Send + Sync + Sized {
    /// Create a new storage
    ///
    /// # Arguments
    /// * `config` - Configuration for the storage
    ///
    fn new(
        config: &PipelineStorageConfiguration,
    ) -> impl Future<Output = Result<Self, StorageError>> + Send;

    /// Get the search parameters
    ///
    fn get_search_parameters(
        &self,
        uuid: &str,
    ) -> impl Future<Output = Result<Option<SearchParameters>, StorageError>> + Send;

    /// Set the pipeline configuration
    ///
    fn set_search_parameters(
        &self,
        uuid: &str,
        params: SearchParameters,
    ) -> impl Future<Output = Result<(), StorageError>> + Send;

    /// Remove the pipeline configuration
    ///
    fn remove_search_params(
        &self,
        uuid: &str,
    ) -> impl Future<Output = Result<(), StorageError>> + Send;

    /// Get the PTM reader
    ///
    fn get_ptms(
        &self,
        uuid: &str,
    ) -> impl Future<Output = Result<Option<Vec<PostTranslationalModification>>, StorageError>> + Send;

    /// Set the PTM reader
    ///
    fn set_ptms(
        &self,
        uuid: &str,
        ptms: &[PostTranslationalModification],
    ) -> impl Future<Output = Result<(), StorageError>> + Send;

    /// Remove PTMs
    ///
    fn remove_ptms(&self, uuid: &str) -> impl Future<Output = Result<(), StorageError>> + Send;

    /// Get comet config
    ///
    fn get_comet_config(
        &self,
        uuid: &str,
    ) -> impl Future<Output = Result<Option<CometConfiguration>, StorageError>> + Send;

    /// Set comet config
    ///
    fn set_comet_config(
        &self,
        uuid: &str,
        config: &CometConfiguration,
    ) -> impl Future<Output = Result<(), StorageError>> + Send;

    /// Remove comet config
    ///
    fn remove_comet_config(
        &self,
        uuid: &str,
    ) -> impl Future<Output = Result<(), StorageError>> + Send;

    fn get_search_parameters_key(uuid: &str) -> String {
        format!("search_parameter:{}", uuid)
    }

    fn get_ptms_key(uuid: &str) -> String {
        format!("ptms:{}", uuid)
    }

    fn get_comet_config_key(uuid: &str) -> String {
        format!("comet_config:{}", uuid)
    }

    /// Get the total search count key
    ///
    /// # Arguments
    /// * `uuid` - UUID for the counters
    ///
    fn get_total_spectrum_count_key(uuid: &str) -> String {
        format!("{}{}", TOTAL_SPECTRUM_COUNT_PREFIX, uuid)
    }

    /// Get the finished search count key
    ///
    /// # Arguments
    /// * `uuid` - UUID for the counters
    fn get_finished_spectrum_count_key(uuid: &str) -> String {
        format!("{}{}", FINISHED_SPECTRUM_COUNT_PREFIX, uuid)
    }

    /// Initialize a keys for new search
    ///
    /// # Arguments
    /// * `uuid` - UUID for the counters
    /// * `search_parameters` - Search parameters
    /// * `ptms` - Post translational modifications
    /// * `comet_config` - Comet configuration
    ///
    fn init_search(
        &self,
        uuid: &str,
        search_parameters: SearchParameters,
        ptms: &[PostTranslationalModification],
        comet_config: &CometConfiguration,
    ) -> impl Future<Output = Result<(), StorageError>> + Send {
        async {
            // Set configs
            self.set_search_parameters(uuid, search_parameters).await?;
            self.set_ptms(uuid, ptms).await?;
            self.set_comet_config(uuid, comet_config).await?;
            // Set counters
            self.init_total_spectrum_count(uuid).await?;
            self.init_finished_spectrum_count(uuid).await?;

            Ok(())
        }
    }

    /// Remove counters for the given search
    ///
    /// # Arguments
    /// * `uuid` - UUID for the counters
    ///
    fn cleanup_search(&self, uuid: &str) -> impl Future<Output = Result<(), StorageError>> + Send {
        async {
            // Remove configs
            self.remove_search_params(uuid).await?;
            self.remove_ptms(uuid).await?;
            self.remove_comet_config(uuid).await?;
            // Remove counters
            self.remove_total_spectrum_count(uuid).await?;
            self.remove_finished_spectrum_count(uuid).await?;
            Ok(())
        }
    }

    /// Initialize total search count
    ///
    /// # Arguments
    /// * `uuid` - UUID for the counters
    ///
    fn init_total_spectrum_count(
        &self,
        uuid: &str,
    ) -> impl Future<Output = Result<(), StorageError>> + Send;

    /// Increment the total search count by
    ///
    /// # Arguments
    /// * `uuid` - UUID for the counters
    /// * `amount` - Amount to increment
    ///
    fn increase_total_spectrum_count(
        &self,
        uuid: &str,
        value: u64,
    ) -> impl Future<Output = Result<(), StorageError>> + Send;

    /// Get the total search count
    ///
    /// # Arguments
    /// * `uuid` - UUID for the counters
    ///
    fn get_total_spectrum_count(
        &self,
        uuid: &str,
    ) -> impl Future<Output = Result<u64, StorageError>> + Send;

    /// Remove the total spectrum count
    ///
    /// # Arguments
    /// * `uuid` - UUID for the counters
    ///
    fn remove_total_spectrum_count(
        &self,
        uuid: &str,
    ) -> impl Future<Output = Result<(), StorageError>> + Send;

    /// Initialize finished search count
    ///
    /// # Arguments
    /// * `uuid` - UUID for the counters
    ///
    fn init_finished_spectrum_count(
        &self,
        uuid: &str,
    ) -> impl Future<Output = Result<(), StorageError>> + Send;

    /// Increment the finished search count by 1
    ///
    /// # Arguments
    /// * `uuid` - UUID for the counters
    ///
    fn increase_finished_spectrum_count(
        &self,
        uuid: &str,
    ) -> impl Future<Output = Result<u64, StorageError>> + Send;

    /// Get the finished search count
    ///
    /// # Arguments
    /// * `uuid` - UUID for the counters
    ///
    fn get_finished_spectrum_count(
        &self,
        uuid: &str,
    ) -> impl Future<Output = Result<u64, StorageError>> + Send;

    /// Remove the finished spectrum count
    ///
    /// # Arguments
    /// * `uuid` - UUID for the counters
    ///
    fn remove_finished_spectrum_count(
        &self,
        uuid: &str,
    ) -> impl Future<Output = Result<(), StorageError>> + Send;

    /// Checks if all spectra are finished
    ///
    /// # Arguments
    /// * `uuid` - UUID for the counters
    ///
    fn is_search_finished(
        &self,
        uuid: &str,
    ) -> impl Future<Output = Result<bool, StorageError>> + Send {
        async {
            let total_count = self.get_total_spectrum_count(uuid).await?;
            let finished_count = self.get_finished_spectrum_count(uuid).await?;
            Ok(total_count == finished_count)
        }
    }
}

/// Local storage for the pipeline to
///
pub struct LocalPipelineStorage {
    /// Pipeline configuration
    search_parameters: RwLock<HashMap<String, SearchParameters>>,

    /// Post translational modifications
    ptms_collections: RwLock<HashMap<String, Vec<PostTranslationalModification>>>,

    /// Comet configurations
    comet_configs: RwLock<HashMap<String, CometConfiguration>>,

    /// Counts
    counters: RwLock<HashMap<String, AtomicU64>>,
}

impl PipelineStorage for LocalPipelineStorage {
    async fn new(_config: &PipelineStorageConfiguration) -> Result<Self, StorageError> {
        Ok(Self {
            search_parameters: RwLock::new(HashMap::new()),
            ptms_collections: RwLock::new(HashMap::new()),
            comet_configs: RwLock::new(HashMap::new()),
            counters: RwLock::new(HashMap::new()),
        })
    }

    async fn get_search_parameters(
        &self,
        uuid: &str,
    ) -> Result<Option<SearchParameters>, StorageError> {
        Ok(self
            .search_parameters
            .read()
            .map_err(|_| LocalStorageError::PoisenedStorageLock("search parameters".to_string()))?
            .get(&Self::get_search_parameters_key(uuid))
            .cloned())
    }

    async fn set_search_parameters(
        &self,
        uuid: &str,
        params: SearchParameters,
    ) -> Result<(), StorageError> {
        let mut search_parameters_guard = self.search_parameters.write().map_err(|_| {
            LocalStorageError::PoisenedStorageLock("reading search parameters".to_string())
        })?;
        search_parameters_guard.insert(Self::get_search_parameters_key(uuid), params);
        Ok(())
    }

    async fn remove_search_params(&self, uuid: &str) -> Result<(), StorageError> {
        let mut search_parameters_guard = self.search_parameters.write().map_err(|_| {
            LocalStorageError::PoisenedStorageLock("setting search parameters".to_string())
        })?;
        search_parameters_guard.remove(&Self::get_search_parameters_key(uuid));
        Ok(())
    }

    async fn get_ptms(
        &self,
        uuid: &str,
    ) -> Result<Option<Vec<PostTranslationalModification>>, StorageError> {
        Ok(self
            .ptms_collections
            .read()
            .map_err(|_| LocalStorageError::PoisenedStorageLock("reading ptms".to_string()))?
            .get(&Self::get_ptms_key(uuid))
            .cloned())
    }

    async fn set_ptms(
        &self,
        uuid: &str,
        ptms: &[PostTranslationalModification],
    ) -> Result<(), StorageError> {
        let mut ptms_guard = self
            .ptms_collections
            .write()
            .map_err(|_| LocalStorageError::PoisenedStorageLock("setting ptms".to_string()))?;
        ptms_guard.insert(Self::get_ptms_key(uuid), ptms.to_vec());
        Ok(())
    }

    async fn remove_ptms(&self, uuid: &str) -> Result<(), StorageError> {
        let mut ptms_guard = self
            .ptms_collections
            .write()
            .map_err(|_| LocalStorageError::PoisenedStorageLock("removing ptms".to_string()))?;
        ptms_guard.remove(&Self::get_ptms_key(uuid));
        Ok(())
    }

    async fn get_comet_config(
        &self,
        uuid: &str,
    ) -> Result<Option<CometConfiguration>, StorageError> {
        Ok(self
            .comet_configs
            .read()
            .map_err(|_| {
                LocalStorageError::PoisenedStorageLock("reading comet configuration".to_string())
            })?
            .get(&Self::get_comet_config_key(uuid))
            .cloned())
    }

    async fn set_comet_config(
        &self,
        uuid: &str,
        config: &CometConfiguration,
    ) -> Result<(), StorageError> {
        let mut comet_config_guards = self
            .comet_configs
            .write()
            .map_err(|_| LocalStorageError::PoisenedStorageLock("setting ptms".to_string()))?;
        comet_config_guards.insert(Self::get_comet_config_key(uuid), config.clone());
        Ok(())
    }

    async fn remove_comet_config(&self, uuid: &str) -> Result<(), StorageError> {
        let mut comet_config_guards = self
            .comet_configs
            .write()
            .map_err(|_| LocalStorageError::PoisenedStorageLock("setting ptms".to_string()))?;
        comet_config_guards.remove(&Self::get_comet_config_key(uuid));
        Ok(())
    }

    async fn init_total_spectrum_count(&self, uuid: &str) -> Result<(), StorageError> {
        self.counters
            .write()
            .map_err(|_| {
                LocalStorageError::PoisenedStorageLock(
                    "initializing total spectrum count".to_string(),
                )
            })?
            .insert(Self::get_total_spectrum_count_key(uuid), AtomicU64::new(0));
        Ok(())
    }

    async fn increase_total_spectrum_count(
        &self,
        uuid: &str,
        value: u64,
    ) -> Result<(), StorageError> {
        self.counters
            .read()
            .map_err(|_| {
                LocalStorageError::PoisenedStorageLock("setting total spectrum count".to_string())
            })?
            .get(&Self::get_total_spectrum_count_key(uuid))
            .ok_or_else(|| {
                LocalStorageError::CountNotFoundError(
                    Self::get_total_spectrum_count_key(uuid).to_string(),
                )
            })?
            .fetch_add(value, Ordering::Relaxed);
        Ok(())
    }

    async fn get_total_spectrum_count(&self, uuid: &str) -> Result<u64, StorageError> {
        Ok(self
            .counters
            .read()
            .map_err(|_| {
                LocalStorageError::PoisenedStorageLock("getting total spectrum count".to_string())
            })?
            .get(&Self::get_total_spectrum_count_key(uuid))
            .ok_or_else(|| {
                LocalStorageError::CountNotFoundError(
                    Self::get_total_spectrum_count_key(uuid).to_string(),
                )
            })?
            .load(Ordering::Relaxed))
    }

    async fn remove_total_spectrum_count(&self, uuid: &str) -> Result<(), StorageError> {
        self.counters
            .write()
            .map_err(|_| {
                LocalStorageError::PoisenedStorageLock("removing total spectrum count".to_string())
            })?
            .remove(&Self::get_total_spectrum_count_key(uuid));
        Ok(())
    }

    async fn init_finished_spectrum_count(&self, uuid: &str) -> Result<(), StorageError> {
        self.counters
            .write()
            .map_err(|_| {
                LocalStorageError::PoisenedStorageLock(
                    "initializing finshed spectrum count".to_string(),
                )
            })?
            .insert(
                Self::get_finished_spectrum_count_key(uuid),
                AtomicU64::new(0),
            );
        Ok(())
    }

    async fn increase_finished_spectrum_count(&self, uuid: &str) -> Result<u64, StorageError> {
        Ok(self
            .counters
            .read()
            .map_err(|_| {
                LocalStorageError::PoisenedStorageLock(
                    "increasing finished spectrum count".to_string(),
                )
            })?
            .get(&Self::get_finished_spectrum_count_key(uuid))
            .ok_or_else(|| {
                LocalStorageError::CountNotFoundError(
                    Self::get_finished_spectrum_count_key(uuid).to_string(),
                )
            })?
            .fetch_add(1, Ordering::Relaxed))
    }

    async fn get_finished_spectrum_count(&self, uuid: &str) -> Result<u64, StorageError> {
        Ok(self
            .counters
            .read()
            .map_err(|_| {
                LocalStorageError::PoisenedStorageLock(
                    "getting finished spectrum count".to_string(),
                )
            })?
            .get(&Self::get_finished_spectrum_count_key(uuid))
            .ok_or_else(|| {
                LocalStorageError::CountNotFoundError(
                    Self::get_finished_spectrum_count_key(uuid).to_string(),
                )
            })?
            .load(Ordering::Relaxed))
    }

    async fn remove_finished_spectrum_count(&self, uuid: &str) -> Result<(), StorageError> {
        self.counters
            .write()
            .map_err(|_| {
                LocalStorageError::PoisenedStorageLock(
                    "removing finished spectrum count".to_string(),
                )
            })?
            .remove(&Self::get_finished_spectrum_count_key(uuid));
        Ok(())
    }
}

pub struct RedisPipelineStorage {
    client: rustis::client::Client,
}

impl RedisPipelineStorage {
    /// Safely converts i64 to u64 so i64::MIN mapsto u64::MIN & i64::MAX maps to u64::MAX
    ///
    /// # Arguments
    /// * `value` - i64 value
    ///
    fn safe_i64_to_u64(value: i64) -> u64 {
        (value as u64).wrapping_add(1 << 63)
    }

    /// Safely converts u64 to i64 so u64::MIN maps to i64::MIN & u64::MAX maps to i64::MAX
    ///
    /// # Arguments
    /// * `value` - u64 value
    ///
    fn safe_u64_to_i64(value: u64) -> i64 {
        (value as i64).wrapping_add(1 << 63)
    }
}

impl PipelineStorage for RedisPipelineStorage {
    async fn new(config: &PipelineStorageConfiguration) -> Result<Self, StorageError> {
        if config.redis_url.is_none() {
            return Err(RedisStorageError::RedisUrlMissing.into());
        }

        let mut redis_client_config =
            rustis::client::Config::from_str(config.redis_url.as_ref().unwrap())
                .map_err(RedisStorageError::ConfigError)?;
        redis_client_config.retry_on_error = true;
        redis_client_config.reconnection = rustis::client::ReconnectionConfig::new_constant(0, 5);

        let client = rustis::client::Client::connect(redis_client_config)
            .await
            .map_err(RedisStorageError::ConnectionError)?;

        Ok(Self { client })
    }

    async fn get_search_parameters(
        &self,
        uuid: &str,
    ) -> Result<Option<SearchParameters>, StorageError> {
        let params_json: String = self
            .client
            .get(Self::get_search_parameters_key(uuid))
            .await
            .map_err(|err| RedisStorageError::RedisError("getting search parameters", err))?;

        if params_json.is_empty() {
            return Ok(None);
        }

        let params: SearchParameters = serde_json::from_str(&params_json).map_err(|err| {
            RedisStorageError::DeserializationError("getting search parameters".to_string(), err)
        })?;

        Ok(Some(params))
    }

    async fn set_search_parameters(
        &self,
        uuid: &str,
        params: SearchParameters,
    ) -> Result<(), StorageError> {
        let params_json = serde_json::to_string(&params).map_err(|err| {
            RedisStorageError::SerializationError("setting search parameters".to_string(), err)
        })?;

        Ok(self
            .client
            .set(Self::get_search_parameters_key(uuid), params_json)
            .await
            .map_err(|err| RedisStorageError::RedisError("setting search parameters", err))?)
    }

    async fn remove_search_params(&self, uuid: &str) -> Result<(), StorageError> {
        self.client
            .del(Self::get_search_parameters_key(uuid))
            .await
            .map_err(|err| RedisStorageError::RedisError("removing search parameters", err))?;
        Ok(())
    }

    async fn get_ptms(
        &self,
        uuid: &str,
    ) -> Result<Option<Vec<PostTranslationalModification>>, StorageError> {
        let ptms_json: String = self
            .client
            .get(Self::get_ptms_key(uuid))
            .await
            .map_err(|err| RedisStorageError::RedisError("getting PTMs", err))?;

        if ptms_json.is_empty() {
            return Ok(None);
        }

        let ptms: Vec<PostTranslationalModification> =
            serde_json::from_str(&ptms_json).map_err(|err| {
                RedisStorageError::DeserializationError("getting PTMs".to_string(), err)
            })?;

        Ok(Some(ptms))
    }

    async fn set_ptms(
        &self,
        uuid: &str,
        ptms: &[PostTranslationalModification],
    ) -> Result<(), StorageError> {
        let ptms_json = serde_json::to_string(ptms).map_err(|err| {
            RedisStorageError::SerializationError("setting ptms".to_string(), err)
        })?;

        Ok(self
            .client
            .set(Self::get_ptms_key(uuid), ptms_json)
            .await
            .map_err(|err| RedisStorageError::RedisError("setting PTMs", err))?)
    }

    async fn remove_ptms(&self, uuid: &str) -> Result<(), StorageError> {
        self.client
            .del(Self::get_ptms_key(uuid))
            .await
            .map_err(|err| RedisStorageError::RedisError("removing PTMs", err))?;
        Ok(())
    }

    async fn get_comet_config(
        &self,
        uuid: &str,
    ) -> Result<Option<CometConfiguration>, StorageError> {
        let comet_params_json: String = self
            .client
            .get(Self::get_comet_config_key(uuid))
            .await
            .map_err(|err| RedisStorageError::RedisError("getting comet configuration", err))?;

        if comet_params_json.is_empty() {
            return Ok(None);
        }

        let config: CometConfiguration =
            serde_json::from_str(&comet_params_json).map_err(|err| {
                RedisStorageError::DeserializationError(
                    "getting comet configuration".to_string(),
                    err,
                )
            })?;

        Ok(Some(config))
    }

    async fn set_comet_config(
        &self,
        uuid: &str,
        config: &CometConfiguration,
    ) -> Result<(), StorageError> {
        let comet_params_json = serde_json::to_string(config).map_err(|err| {
            RedisStorageError::SerializationError("setting comet configuration".to_string(), err)
        })?;

        Ok(self
            .client
            .set(Self::get_comet_config_key(uuid), comet_params_json)
            .await
            .map_err(|err| RedisStorageError::RedisError("setting comet configuration", err))?)
    }

    async fn remove_comet_config(&self, uuid: &str) -> Result<(), StorageError> {
        self.client
            .del(Self::get_comet_config_key(uuid))
            .await
            .map_err(|err| RedisStorageError::RedisError("removing comet configuration", err))?;
        Ok(())
    }

    async fn init_total_spectrum_count(&self, uuid: &str) -> Result<(), StorageError> {
        Ok(self
            .client
            .set(Self::get_total_spectrum_count_key(uuid), 0_i64)
            .await
            .map_err(|err| {
                RedisStorageError::RedisError("initializing total spectrum count", err)
            })?)
    }

    async fn increase_total_spectrum_count(
        &self,
        uuid: &str,
        value: u64,
    ) -> Result<(), StorageError> {
        let value_i64 = Self::safe_u64_to_i64(value);
        self.client
            .incrby(Self::get_total_spectrum_count_key(uuid), value_i64)
            .await
            .map_err(|err| RedisStorageError::RedisError("increase total spectrum count", err))?;
        Ok(())
    }

    async fn get_total_spectrum_count(&self, uuid: &str) -> Result<u64, StorageError> {
        let value_i64 = self
            .client
            .get(Self::get_total_spectrum_count_key(uuid))
            .await
            .map_err(|err| RedisStorageError::RedisError("getting total spectrum count", err))?;
        Ok(Self::safe_i64_to_u64(value_i64))
    }

    async fn remove_total_spectrum_count(&self, uuid: &str) -> Result<(), StorageError> {
        self.client
            .del(Self::get_total_spectrum_count_key(uuid))
            .await
            .map_err(|err| RedisStorageError::RedisError("removing total spectrum count", err))?;
        Ok(())
    }

    async fn init_finished_spectrum_count(&self, uuid: &str) -> Result<(), StorageError> {
        Ok(self
            .client
            .set(Self::get_finished_spectrum_count_key(uuid), 0_i64)
            .await
            .map_err(|err| {
                RedisStorageError::RedisError("initializing total spectrum count", err)
            })?)
    }

    async fn increase_finished_spectrum_count(&self, uuid: &str) -> Result<u64, StorageError> {
        let value_i64 = self
            .client
            .incr(Self::get_total_spectrum_count_key(uuid))
            .await
            .map_err(|err| {
                RedisStorageError::RedisError("increasing finished spectrum count", err)
            })?;
        Ok(Self::safe_i64_to_u64(value_i64))
    }

    async fn get_finished_spectrum_count(&self, uuid: &str) -> Result<u64, StorageError> {
        let value_i64 = self
            .client
            .get(Self::get_finished_spectrum_count_key(uuid))
            .await
            .map_err(|err| RedisStorageError::RedisError("getting finished spectrum count", err))?;
        Ok(Self::safe_i64_to_u64(value_i64))
    }

    async fn remove_finished_spectrum_count(&self, uuid: &str) -> Result<(), StorageError> {
        self.client
            .del(Self::get_finished_spectrum_count_key(uuid))
            .await
            .map_err(|err| {
                RedisStorageError::RedisError("removing finished spectrum count", err)
            })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_safe_i64_to_u64() {
        assert_eq!(RedisPipelineStorage::safe_i64_to_u64(i64::MIN), u64::MIN);
        assert_eq!(RedisPipelineStorage::safe_i64_to_u64(i64::MAX), u64::MAX);
    }

    #[test]
    fn test_safe_u64_to_i64() {
        assert_eq!(RedisPipelineStorage::safe_u64_to_i64(u64::MIN), i64::MIN);
        assert_eq!(RedisPipelineStorage::safe_u64_to_i64(u64::MAX), i64::MAX);
    }
}
