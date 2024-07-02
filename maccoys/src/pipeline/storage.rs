// std imports
use std::{collections::HashMap, str::FromStr};

// 3rd party imports
use anyhow::{bail, Context, Result};
use dihardts_omicstools::proteomics::post_translational_modifications::PostTranslationalModification;
use futures::Future;
use rustis::commands::{GenericCommands, StringCommands};

// local imports
use super::configuration::PipelineConfiguration;
use crate::io::comet::configuration::Configuration as CometConfiguration;

/// Central storage for configuration, PTM etc.
///
pub trait PipelineStorage: Send + Sync + Sized {
    /// Create a new storage
    ///
    /// # Arguments
    /// * `config` - Configuration for the storage
    ///
    fn new(config: &PipelineConfiguration) -> impl Future<Output = Result<Self>> + Send;

    /// Get the pipeline configuration
    ///
    fn get_configuration(
        &self,
        uuid: &str,
    ) -> impl Future<Output = Result<Option<PipelineConfiguration>>> + Send;

    /// Set the pipeline configuration
    ///
    fn set_configuration(
        &mut self,
        uuid: &str,
        config: &PipelineConfiguration,
    ) -> impl Future<Output = Result<()>> + Send;

    /// Remove the pipeline configuration
    ///
    fn remove_configuration(&mut self, uuid: &str) -> impl Future<Output = Result<()>> + Send;

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
        ptms: &Vec<PostTranslationalModification>,
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

    fn get_configuration_key(uuid: &str) -> String {
        format!("config:{}", uuid)
    }

    fn get_ptms_key(uuid: &str) -> String {
        format!("ptms:{}", uuid)
    }

    fn get_comet_config_key(uuid: &str) -> String {
        format!("comet_config:{}", uuid)
    }
}

/// Local storage for the pipeline to
///
pub struct LocalPipelineStorage {
    /// Pipeline configuration
    configs: HashMap<String, PipelineConfiguration>,

    /// Post translational modifications
    ptms_collections: HashMap<String, Vec<PostTranslationalModification>>,

    /// Comet configurations
    comet_configs: HashMap<String, CometConfiguration>,
}

impl PipelineStorage for LocalPipelineStorage {
    async fn new(_config: &PipelineConfiguration) -> Result<Self> {
        Ok(Self {
            configs: HashMap::new(),
            ptms_collections: HashMap::new(),
            comet_configs: HashMap::new(),
        })
    }

    async fn get_configuration(&self, uuid: &str) -> Result<Option<PipelineConfiguration>> {
        Ok(self
            .configs
            .get(&Self::get_configuration_key(uuid))
            .cloned())
    }

    async fn set_configuration(
        &mut self,
        uuid: &str,
        config: &PipelineConfiguration,
    ) -> Result<()> {
        self.configs
            .insert(Self::get_configuration_key(uuid), config.clone());
        Ok(())
    }

    async fn remove_configuration(&mut self, uuid: &str) -> Result<()> {
        self.configs.remove(&Self::get_configuration_key(uuid));
        Ok(())
    }

    async fn get_ptms(&self, uuid: &str) -> Result<Option<Vec<PostTranslationalModification>>> {
        Ok(self
            .ptms_collections
            .get(&Self::get_ptms_key(uuid))
            .cloned())
    }

    async fn set_ptms(
        &mut self,
        uuid: &str,
        ptms: &Vec<PostTranslationalModification>,
    ) -> Result<()> {
        self.ptms_collections
            .insert(Self::get_ptms_key(uuid), ptms.clone());
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
}

pub struct RedisPipelineStorage {
    client: rustis::client::Client,
}

impl PipelineStorage for RedisPipelineStorage {
    async fn new(config: &PipelineConfiguration) -> Result<Self> {
        if config.pipelines.redis_url.is_none() {
            bail!("[STORAGE] Redis URL is None")
        }

        let mut redis_client_config =
            rustis::client::Config::from_str(config.pipelines.redis_url.as_ref().unwrap())?;
        redis_client_config.retry_on_error = true;
        redis_client_config.reconnection = rustis::client::ReconnectionConfig::new_constant(0, 5);

        let client = rustis::client::Client::connect(redis_client_config)
            .await
            .context("[STORAGE] Error opening connection to Redis")?;

        Ok(Self { client })
    }

    async fn get_configuration(&self, uuid: &str) -> Result<Option<PipelineConfiguration>> {
        let config_json: String = self
            .client
            .get(Self::get_configuration_key(uuid))
            .await
            .context("[STORAGE] Error getting configuration")?;

        if config_json.is_empty() {
            return Ok(None);
        }

        let config: PipelineConfiguration = serde_json::from_str(&config_json)
            .context("[STORAGE] Error deserializing configuration")?;

        Ok(Some(config))
    }

    async fn set_configuration(
        &mut self,
        uuid: &str,
        config: &PipelineConfiguration,
    ) -> Result<()> {
        let config_json =
            serde_json::to_string(config).context("[STORAGE] Error serializing config")?;

        self.client
            .set(Self::get_configuration_key(uuid), config_json)
            .await
            .context("[STORAGE] Error setting configuration")
    }

    async fn remove_configuration(&mut self, uuid: &str) -> Result<()> {
        self.client
            .del(Self::get_configuration_key(uuid))
            .await
            .context("[STORAGE] Error removing configuration")?;
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

    async fn set_ptms(
        &mut self,
        uuid: &str,
        ptms: &Vec<PostTranslationalModification>,
    ) -> Result<()> {
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
            .context("[STORAGE] Error getting PTMs")?;

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
}
