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
use mini_moka::sync::Cache;
use rustis::commands::{GenericCommands, StringCommands};

use super::{
    configuration::{PipelineStorageConfiguration, SearchParameters},
    errors::storage_error::{LocalStorageError, RedisStorageError, StorageError},
};

const TOTAL_SPECTRUM_COUNT_PREFIX: &str = "total_spectrum_count_";
const FINISHED_SPECTRUM_COUNT_PREFIX: &str = "finished_spectrum_count_";

/// Max AtomicU64 as default value if key was not found
static MAX_ATOMIC_U64: AtomicU64 = AtomicU64::new(u64::MAX);

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

    fn get_search_parameters_key(uuid: &str) -> String {
        format!("search_parameter:{}", uuid)
    }

    fn get_ptms_key(uuid: &str) -> String {
        format!("ptms:{}", uuid)
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
    /// * `xcorr_config` - Comet configuration
    ///
    fn init_search(
        &self,
        uuid: &str,
        search_parameters: SearchParameters,
        ptms: &[PostTranslationalModification],
    ) -> impl Future<Output = Result<(), StorageError>> + Send {
        async {
            // Set configs
            self.set_search_parameters(uuid, search_parameters).await?;
            self.set_ptms(uuid, ptms).await?;
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
    ) -> impl Future<Output = Result<(), StorageError>> + Send;

    /// Get the total search count. If the count is not initialized, it will return u64::MAX.
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

    /// Get the finished search count. If the count is not initialized, it will return u64::MAX.
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
            Ok(total_count == finished_count && self.is_search_fully_enqueued(uuid).await?)
        }
    }

    /// Returns key for done flag
    ///
    /// # Arguments
    /// * `uuid` - UUID for the counters
    ///
    fn get_search_fully_enqueued_key(uuid: &str) -> String {
        format!("search_fully_enqueued:{}", uuid)
    }

    /// Sets flag that all spectra are enqueued for the search
    /// Implementations needs to make sure, that the flag is deleted after a time to idle (time after last insert or get)
    ///
    /// # Arguments
    /// * `uuid` - UUID for the counters
    ///
    fn set_search_fully_enqueued(
        &self,
        uuid: &str,
    ) -> impl Future<Output = Result<(), StorageError>> + Send;

    /// Gets the fully enqueued flag for a search
    /// Implementations needs to make sure, that the flag is deleted after a time to idle (time after last insert or get)
    ///
    /// # Arguments
    /// * `uuid` - UUID for the counters
    ///
    fn is_search_fully_enqueued(
        &self,
        uuid: &str,
    ) -> impl Future<Output = Result<bool, StorageError>> + Send;
}

/// Local storage for the pipeline to
///
pub struct LocalPipelineStorage {
    /// Pipeline configuration
    search_parameters: RwLock<HashMap<String, SearchParameters>>,

    /// Post translational modifications
    ptms_collections: RwLock<HashMap<String, Vec<PostTranslationalModification>>>,

    /// Counts
    counters: RwLock<HashMap<String, AtomicU64>>,

    /// Flags
    flags: Cache<String, bool>,
}

impl PipelineStorage for LocalPipelineStorage {
    async fn new(config: &PipelineStorageConfiguration) -> Result<Self, StorageError> {
        Ok(Self {
            search_parameters: RwLock::new(HashMap::new()),
            ptms_collections: RwLock::new(HashMap::new()),
            counters: RwLock::new(HashMap::new()),
            flags: Cache::builder()
                .max_capacity(1000)
                .time_to_idle(std::time::Duration::from_secs(config.time_to_idle))
                .build(),
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

    async fn increase_total_spectrum_count(&self, uuid: &str) -> Result<(), StorageError> {
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
            .fetch_add(1, Ordering::Relaxed);
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
            .unwrap_or(&MAX_ATOMIC_U64)
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
            .unwrap_or(&MAX_ATOMIC_U64)
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

    async fn set_search_fully_enqueued(&self, uuid: &str) -> Result<(), StorageError> {
        self.flags
            .insert(Self::get_search_fully_enqueued_key(uuid), true);
        Ok(())
    }

    async fn is_search_fully_enqueued(&self, uuid: &str) -> Result<bool, StorageError> {
        Ok(self
            .flags
            .get(&Self::get_search_fully_enqueued_key(uuid))
            .unwrap_or(false))
    }
}

pub struct RedisPipelineStorage {
    client: rustis::client::Client,
    time_to_idle: u64,
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

        Ok(Self {
            client,
            time_to_idle: config.time_to_idle,
        })
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

    async fn init_total_spectrum_count(&self, uuid: &str) -> Result<(), StorageError> {
        Ok(self
            .client
            .set(
                Self::get_total_spectrum_count_key(uuid),
                Self::safe_u64_to_i64(0),
            )
            .await
            .map_err(|err| {
                RedisStorageError::RedisError("initializing total spectrum count", err)
            })?)
    }

    async fn increase_total_spectrum_count(&self, uuid: &str) -> Result<(), StorageError> {
        self.client
            .incr(Self::get_total_spectrum_count_key(uuid))
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
            .set(
                Self::get_finished_spectrum_count_key(uuid),
                Self::safe_u64_to_i64(0),
            )
            .await
            .map_err(|err| {
                RedisStorageError::RedisError("initializing total spectrum count", err)
            })?)
    }

    async fn increase_finished_spectrum_count(&self, uuid: &str) -> Result<u64, StorageError> {
        let value_i64 = self
            .client
            .incr(Self::get_finished_spectrum_count_key(uuid))
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

    async fn set_search_fully_enqueued(&self, uuid: &str) -> Result<(), StorageError> {
        let key = Self::get_search_fully_enqueued_key(uuid);

        self.client
            .set(&key, true)
            .await
            .map_err(|err| RedisStorageError::RedisError("setting search done flag", err))?;

        self.client
            .expire(
                &key,
                self.time_to_idle,
                rustis::commands::ExpireOption::None,
            )
            .await
            .map_err(|err| RedisStorageError::RedisError("setting search done flag expiry", err))?;

        Ok(())
    }

    async fn is_search_fully_enqueued(&self, uuid: &str) -> Result<bool, StorageError> {
        let key = Self::get_search_fully_enqueued_key(uuid);
        // Get the value of the search done flag
        let value: Option<bool> = self
            .client
            .get(&key)
            .await
            .map_err(|err| RedisStorageError::RedisError("getting search done flag", err))?;

        // If the value exists, we extend its expiry time
        match value {
            Some(v) => {
                self.client
                    .expire(&key, self.time_to_idle, rustis::commands::ExpireOption::Gt)
                    .await
                    .map_err(|err| {
                        RedisStorageError::RedisError("setting search done flag expiry", err)
                    })?;
                Ok(v)
            }
            None => Ok(false), // If the key does not exist, we consider the search not done
        }
    }
}

#[cfg(test)]
mod tests {
    use serial_test::serial;

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

    #[tokio::test]
    #[serial]
    async fn test_redis_counting() {
        let time_to_idle: u64 = 5;
        let uuid: &'static str = "test_uuid";

        let config: PipelineStorageConfiguration = PipelineStorageConfiguration {
            time_to_idle,
            redis_url: Some("redis://127.0.0.1:6380".to_string()),
        };

        let storage = RedisPipelineStorage::new(&config).await.unwrap();
        // start fresh
        storage.client.del("*").await.unwrap();

        // Initialize the counters
        storage.init_total_spectrum_count(uuid).await.unwrap();
        storage.init_finished_spectrum_count(uuid).await.unwrap();

        // Check that the counters are initialized to 0
        assert_eq!(0, storage.get_total_spectrum_count(uuid).await.unwrap());
        assert_eq!(0, storage.get_finished_spectrum_count(uuid).await.unwrap());

        // Increase the total spectrum count
        for i in 1..10 {
            storage.increase_total_spectrum_count(uuid).await.unwrap();

            assert_eq!(i, storage.get_total_spectrum_count(uuid).await.unwrap());
        }

        // Increase the finished spectrum count
        for i in 1..10 {
            storage
                .increase_finished_spectrum_count(uuid)
                .await
                .unwrap();

            assert_eq!(i, storage.get_finished_spectrum_count(uuid).await.unwrap());
        }
    }

    /// Test the time to idle functionality for cvarious storage implementations.s
    async fn test_generic_time_to_idle<S>(storage: S, uuid: &str, time_to_idle: u64)
    where
        S: PipelineStorage,
    {
        // done flag should not exist
        assert!(!storage.is_search_fully_enqueued(uuid).await.unwrap());

        // set done flag
        storage.set_search_fully_enqueued(uuid).await.unwrap();

        // should be exists after half the TTI
        tokio::time::sleep(tokio::time::Duration::from_secs(time_to_idle / 2)).await;
        assert!(storage.is_search_fully_enqueued(uuid).await.unwrap());

        // should not be exists after TTI
        tokio::time::sleep(tokio::time::Duration::from_secs(time_to_idle + 1)).await;
        assert!(!storage.is_search_fully_enqueued(uuid).await.unwrap());
    }

    #[tokio::test]
    #[serial]
    async fn test_redis_time_to_idle() {
        let time_to_idle: u64 = 5;
        let uuid: &'static str = "test_uuid";

        let config: PipelineStorageConfiguration = PipelineStorageConfiguration {
            time_to_idle,
            redis_url: Some("redis://127.0.0.1:6380".to_string()),
        };

        let storage = RedisPipelineStorage::new(&config).await.unwrap();
        // start fresh
        storage.client.del("*").await.unwrap();

        test_generic_time_to_idle(storage, uuid, time_to_idle).await;
    }

    #[tokio::test]
    async fn test_local_time_to_idle() {
        let time_to_idle: u64 = 5;
        let uuid: &'static str = "test_uuid";

        let storage = LocalPipelineStorage::new(&PipelineStorageConfiguration {
            time_to_idle,
            redis_url: None,
        })
        .await
        .unwrap();

        test_generic_time_to_idle(storage, uuid, time_to_idle).await;
    }
}
