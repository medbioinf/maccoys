// std imports
use std::{
    collections::HashMap,
    str::FromStr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

// 3rd party imports
use anyhow::{bail, Context, Result};
use dihardts_omicstools::proteomics::post_translational_modifications::PostTranslationalModification;
use futures::Future;
use rustis::commands::{GenericCommands, StringCommands};

// local imports
use crate::io::comet::configuration::Configuration as CometConfiguration;

use super::configuration::{PipelineStorageConfiguration, SearchParameters};

/// Central storage for configuration, PTM etc.
///
pub trait PipelineStorage: Send + Sync + Sized {
    /// Create a new storage
    ///
    /// # Arguments
    /// * `config` - Configuration for the storage
    ///
    fn new(config: &PipelineStorageConfiguration) -> impl Future<Output = Result<Self>> + Send;

    /// Get the search parameters
    ///
    fn get_search_parameters(
        &self,
        uuid: &str,
    ) -> impl Future<Output = Result<Option<SearchParameters>>> + Send;

    /// Set the pipeline configuration
    ///
    fn set_search_parameters(
        &mut self,
        uuid: &str,
        params: SearchParameters,
    ) -> impl Future<Output = Result<()>> + Send;

    /// Remove the pipeline configuration
    ///
    fn remove_search_params(&mut self, uuid: &str) -> impl Future<Output = Result<()>> + Send;

    /// Get the PTM reader
    ///
    fn get_ptms(
        &self,
        uuid: &str,
    ) -> impl Future<Output = Result<Option<Vec<PostTranslationalModification>>>> + Send;

    /// Set the PTM reader
    ///
    fn set_ptms(
        &mut self,
        uuid: &str,
        ptms: &[PostTranslationalModification],
    ) -> impl Future<Output = Result<()>> + Send;

    /// Remove PTMs
    ///
    fn remove_ptms(&mut self, uuid: &str) -> impl Future<Output = Result<()>> + Send;

    /// Get comet config
    ///
    fn get_comet_config(
        &self,
        uuid: &str,
    ) -> impl Future<Output = Result<Option<CometConfiguration>>> + Send;

    /// Set comet config
    ///
    fn set_comet_config(
        &mut self,
        uuid: &str,
        config: &CometConfiguration,
    ) -> impl Future<Output = Result<()>> + Send;

    /// Remove comet config
    ///
    fn remove_comet_config(&mut self, uuid: &str) -> impl Future<Output = Result<()>> + Send;

    /// Initialize the flag that the complete search is enqueued
    ///
    /// # Arguments
    /// * `uuid` - UUID for the counters
    ///
    fn init_is_completely_enqueued(&mut self, uuid: &str) -> impl Future<Output = Result<()>>;

    /// Sets a flag that the complete search is enqueued
    ///
    /// # Arguments
    /// * `uuid` - UUID for the counters
    ///
    fn set_is_completely_enqueued(&mut self, uuid: &str) -> impl Future<Output = Result<()>>;

    /// Remove the flag that the complete search is enqueued
    ///
    /// # Arguments
    /// * `uuid` - UUID for the counters
    ///
    fn remove_is_completely_enqueued(&mut self, uuid: &str) -> impl Future<Output = Result<()>>;

    fn get_search_parameters_key(uuid: &str) -> String {
        format!("search_parameter:{}", uuid)
    }

    fn get_ptms_key(uuid: &str) -> String {
        format!("ptms:{}", uuid)
    }

    fn get_comet_config_key(uuid: &str) -> String {
        format!("comet_config:{}", uuid)
    }

    fn get_is_completely_enqueued_key(uuid: &str) -> String {
        format!("is_completely_enqueued:{}", uuid)
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
        &mut self,
        uuid: &str,
        search_parameters: SearchParameters,
        ptms: &[PostTranslationalModification],
        comet_config: &CometConfiguration,
    ) -> impl Future<Output = Result<()>> {
        async {
            // Set configs
            self.set_search_parameters(uuid, search_parameters).await?;
            self.set_ptms(uuid, ptms).await?;
            self.set_comet_config(uuid, comet_config).await?;
            // Set flags
            self.init_is_completely_enqueued(uuid).await?;

            Ok(())
        }
    }

    /// Remove counters for the given search
    ///
    /// # Arguments
    /// * `uuid` - UUID for the counters
    ///
    fn cleanup_search(&mut self, uuid: &str) -> impl Future<Output = Result<()>> {
        async {
            // Remove configs
            self.remove_search_params(uuid).await?;
            self.remove_ptms(uuid).await?;
            self.remove_comet_config(uuid).await?;
            // Remove flags
            self.remove_is_completely_enqueued(uuid).await?;

            Ok(())
        }
    }
}

/// Local storage for the pipeline to
///
pub struct LocalPipelineStorage {
    /// Pipeline configuration
    search_parameters: HashMap<String, SearchParameters>,

    /// Post translational modifications
    ptms_collections: HashMap<String, Vec<PostTranslationalModification>>,

    /// Comet configurations
    comet_configs: HashMap<String, CometConfiguration>,

    /// Flags
    flags: HashMap<String, Arc<AtomicBool>>,
}

impl PipelineStorage for LocalPipelineStorage {
    async fn new(_config: &PipelineStorageConfiguration) -> Result<Self> {
        Ok(Self {
            search_parameters: HashMap::new(),
            ptms_collections: HashMap::new(),
            comet_configs: HashMap::new(),
            flags: HashMap::new(),
        })
    }

    async fn get_search_parameters(&self, uuid: &str) -> Result<Option<SearchParameters>> {
        Ok(self
            .search_parameters
            .get(&Self::get_search_parameters_key(uuid))
            .cloned())
    }

    async fn set_search_parameters(&mut self, uuid: &str, params: SearchParameters) -> Result<()> {
        self.search_parameters
            .insert(Self::get_search_parameters_key(uuid), params);
        Ok(())
    }

    async fn remove_search_params(&mut self, uuid: &str) -> Result<()> {
        self.search_parameters
            .remove(&Self::get_search_parameters_key(uuid));
        Ok(())
    }

    async fn get_ptms(&self, uuid: &str) -> Result<Option<Vec<PostTranslationalModification>>> {
        Ok(self
            .ptms_collections
            .get(&Self::get_ptms_key(uuid))
            .cloned())
    }

    async fn set_ptms(&mut self, uuid: &str, ptms: &[PostTranslationalModification]) -> Result<()> {
        self.ptms_collections
            .insert(Self::get_ptms_key(uuid), ptms.to_vec());
        Ok(())
    }

    async fn remove_ptms(&mut self, uuid: &str) -> Result<()> {
        self.ptms_collections.remove(&Self::get_ptms_key(uuid));
        Ok(())
    }

    async fn get_comet_config(&self, uuid: &str) -> Result<Option<CometConfiguration>> {
        Ok(self
            .comet_configs
            .get(&Self::get_comet_config_key(uuid))
            .cloned())
    }

    async fn set_comet_config(&mut self, uuid: &str, config: &CometConfiguration) -> Result<()> {
        self.comet_configs
            .insert(Self::get_comet_config_key(uuid), config.clone());
        Ok(())
    }

    async fn remove_comet_config(&mut self, uuid: &str) -> Result<()> {
        self.comet_configs.remove(&Self::get_comet_config_key(uuid));
        Ok(())
    }

    async fn init_is_completely_enqueued(&mut self, uuid: &str) -> Result<()> {
        self.flags.insert(
            Self::get_is_completely_enqueued_key(uuid),
            Arc::new(AtomicBool::new(false)),
        );
        Ok(())
    }

    async fn set_is_completely_enqueued(&mut self, uuid: &str) -> Result<()> {
        match self.flags.get(&Self::get_is_completely_enqueued_key(uuid)) {
            Some(flag) => {
                flag.store(true, Ordering::Relaxed);
                Ok(())
            }
            None => bail!(
                "[STORAGE] Flag {} not found",
                Self::get_is_completely_enqueued_key(uuid)
            ),
        }
    }

    async fn remove_is_completely_enqueued(&mut self, uuid: &str) -> Result<()> {
        match self
            .flags
            .remove(&Self::get_is_completely_enqueued_key(uuid))
        {
            Some(_) => Ok(()),
            None => Ok(()),
        }
    }
}

pub struct RedisPipelineStorage {
    client: rustis::client::Client,
}

impl PipelineStorage for RedisPipelineStorage {
    async fn new(config: &PipelineStorageConfiguration) -> Result<Self> {
        if config.redis_url.is_none() {
            bail!("[STORAGE] Redis URL is None")
        }

        let mut redis_client_config =
            rustis::client::Config::from_str(config.redis_url.as_ref().unwrap())?;
        redis_client_config.retry_on_error = true;
        redis_client_config.reconnection = rustis::client::ReconnectionConfig::new_constant(0, 5);

        let client = rustis::client::Client::connect(redis_client_config)
            .await
            .context("[STORAGE] Error opening connection to Redis")?;

        Ok(Self { client })
    }

    async fn get_search_parameters(&self, uuid: &str) -> Result<Option<SearchParameters>> {
        let params_json: String = self
            .client
            .get(Self::get_search_parameters_key(uuid))
            .await
            .context("[STORAGE] Error getting search params")?;

        if params_json.is_empty() {
            return Ok(None);
        }

        let params: SearchParameters = serde_json::from_str(&params_json)
            .context("[STORAGE] Error deserializing search params")?;

        Ok(Some(params))
    }

    async fn set_search_parameters(&mut self, uuid: &str, params: SearchParameters) -> Result<()> {
        let params_json =
            serde_json::to_string(&params).context("[STORAGE] Error serializing search params")?;

        self.client
            .set(Self::get_search_parameters_key(uuid), params_json)
            .await
            .context("[STORAGE] Error setting configuration")
    }

    async fn remove_search_params(&mut self, uuid: &str) -> Result<()> {
        self.client
            .del(Self::get_search_parameters_key(uuid))
            .await
            .context("[STORAGE] Error removing search params")?;
        Ok(())
    }

    async fn get_ptms(&self, uuid: &str) -> Result<Option<Vec<PostTranslationalModification>>> {
        let ptms_json: String = self
            .client
            .get(Self::get_ptms_key(uuid))
            .await
            .context("[STORAGE] Error getting PTMs")?;

        if ptms_json.is_empty() {
            return Ok(None);
        }

        let ptms: Vec<PostTranslationalModification> =
            serde_json::from_str(&ptms_json).context("[STORAGE] Error deserializing PTMs")?;

        Ok(Some(ptms))
    }

    async fn set_ptms(&mut self, uuid: &str, ptms: &[PostTranslationalModification]) -> Result<()> {
        let ptms_json = serde_json::to_string(ptms).context("[STORAGE] Error serializing PTMs")?;

        self.client
            .set(Self::get_ptms_key(uuid), ptms_json)
            .await
            .context("[STORAGE] Error setting PTMs")
    }

    async fn remove_ptms(&mut self, uuid: &str) -> Result<()> {
        self.client
            .del(Self::get_ptms_key(uuid))
            .await
            .context("[STORAGE] Error removing PTMs")?;
        Ok(())
    }

    async fn get_comet_config(&self, uuid: &str) -> Result<Option<CometConfiguration>> {
        let comet_params_json: String = self
            .client
            .get(Self::get_comet_config_key(uuid))
            .await
            .context("[STORAGE] Error getting Comet configuration")?;

        if comet_params_json.is_empty() {
            return Ok(None);
        }

        let config: CometConfiguration = serde_json::from_str(&comet_params_json)
            .context("[STORAGE] Error deserializing Comet configuration")?;

        Ok(Some(config))
    }

    async fn set_comet_config(&mut self, uuid: &str, config: &CometConfiguration) -> Result<()> {
        let comet_params_json = serde_json::to_string(config)
            .context("[STORAGE] Error serializing Comet configuration")?;

        self.client
            .set(Self::get_comet_config_key(uuid), comet_params_json)
            .await
            .context("[STORAGE] Error setting Comet configuration")
    }

    async fn remove_comet_config(&mut self, uuid: &str) -> Result<()> {
        self.client
            .del(Self::get_comet_config_key(uuid))
            .await
            .context("[STORAGE] Error removing Comet configuration")?;
        Ok(())
    }

    async fn init_is_completely_enqueued(&mut self, uuid: &str) -> Result<()> {
        let key = Self::get_is_completely_enqueued_key(uuid);
        self.client
            .set(key, false)
            .await
            .context("[STORAGE] Error initialize is completely enqueued ")
    }
    async fn set_is_completely_enqueued(&mut self, uuid: &str) -> Result<()> {
        let key = Self::get_is_completely_enqueued_key(uuid);
        self.client
            .set(key, true)
            .await
            .context("[STORAGE] Error setting is completely enqueued")
    }

    async fn remove_is_completely_enqueued(&mut self, uuid: &str) -> Result<()> {
        let key = Self::get_is_completely_enqueued_key(uuid);
        self.client
            .del(key)
            .await
            .context("[STORAGE] Error removing completely enqueued")?;

        Ok(())
    }
}
