// 3rd party imports
use anyhow::Result;
use macpepdb::database::configuration_table::ConfigurationTable as ConfigurationTableTrait;
use macpepdb::database::scylla::{
    client::{Client, GenericClient},
    configuration_table::ConfigurationTable,
    migrations::run_migrations,
};
use macpepdb::entities::configuration::Configuration;
use reqwest;
use serde_json;

pub struct DatabaseBuild {
    database_nodes: Vec<String>,
    database: String,
}

impl DatabaseBuild {
    /// Creates a new DatabaseBuild instance.
    ///
    /// # Arguments
    /// * `database_nodes` - List of database nodes
    /// * `database` - Database/keyspace name
    pub fn new(database_nodes: Vec<String>, database: String) -> Self {
        Self {
            database_nodes,
            database,
        }
    }

    /// Builds a MaCPepDB database for decoy caching.
    ///
    /// # Arguments
    /// * `configuration_resource` - Path or http(s)-URL to a MaCPepDB configuration JSON file. If a URL is given, the file will be downloaded and parsed.
    ///
    pub async fn build(&self, configuration_resource: &str) -> Result<()> {
        let mut client = Client::new(&self.database_nodes, &self.database).await?;

        // Run migrations
        run_migrations(&client).await;

        match ConfigurationTable::select(&mut client).await {
            Ok(_) => {
                tracing::error!("Configuration already exists in database. Nothing to do.");
            }
            _ => {}
        }

        if configuration_resource.starts_with("http://")
            || configuration_resource.starts_with("https://")
        {
            let configuration: Configuration = serde_json::from_str(
                reqwest::get(configuration_resource)
                    .await?
                    .text()
                    .await?
                    .as_str(),
            )?;
            ConfigurationTable::insert(&mut client, &configuration).await?;
        } else {
            let configuration: Configuration =
                serde_json::from_str(&std::fs::read_to_string(configuration_resource)?)?;
            ConfigurationTable::insert(&mut client, &configuration).await?;
        }
        Ok(())
    }
}
