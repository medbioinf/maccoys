// 3rd party imports
use anyhow::Result;
use macpepdb::database::configuration_table::ConfigurationTable as ConfigurationTableTrait;
use macpepdb::database::generic_client::GenericClient;
use macpepdb::database::scylla::{
    client::Client, configuration_table::ConfigurationTable, migrations::run_migrations,
};
use macpepdb::entities::configuration::Configuration;

pub struct DatabaseBuild {
    database_url: String,
}

impl DatabaseBuild {
    /// Creates a new DatabaseBuild instance.
    ///
    /// # Arguments
    /// * `database_url` - Database URL of the format scylla://host1,host2,host3/keyspace
    pub fn new(database_url: &str) -> Self {
        Self {
            database_url: database_url.to_string(),
        }
    }

    /// Builds a MaCPepDB database for decoy caching.
    ///
    /// # Arguments
    /// * `configuration_resource` - Path or http(s)-URL to a MaCPepDB configuration JSON file. If a URL is given, the file will be downloaded and parsed.
    ///
    pub async fn build(&self, configuration_resource: &str) -> Result<()> {
        let mut client = Client::new(&self.database_url).await?;

        // Run migrations
        run_migrations(&client).await?;

        if ConfigurationTable::select(&client).await.is_ok() {
            tracing::error!("Configuration already exists in database. Nothing to do.");
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
