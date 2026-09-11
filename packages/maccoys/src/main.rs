use std::collections::HashMap;
// std imports
use std::env;
use std::fs::{self, read_to_string, write as write_file, File};
use std::io::BufReader;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::Arc;

// 3rd party imports
use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use dihardts_omicstools::proteomics::io::mzml::{index::Index, reader::Reader as MzmlReader};
use glob::glob;
use maccoys::pipeline::configuration::{PipelineConfiguration, RemoteEntypointConfiguration};
use maccoys::pipeline::local_pipeline::{build_and_run_local_pipeline, QueueTarget, StorageTarget};
use maccoys::pipeline::queue_server::QueueServer;
use maccoys::pipeline::remote_pipeline::RemotePipeline;
use maccoys::pipeline::remote_pipeline_web_api::RemotePipelineWebApi;
use maccoys::pipeline::tasks::error_task::{self, ErrorTask};
use maccoys::pipeline::tasks::identification_task::{self, IdentificationTask};
use maccoys::pipeline::tasks::indexing_task::{self, IndexingTask};
use maccoys::pipeline::tasks::publication_task::{self, PublicationTask};
use maccoys::pipeline::tasks::scoring_task::{self, ScoringTask};
use maccoys::pipeline::tasks::search_space_generation_task::{self, SearchSpaceGenerationTask};
use maccoys::search_space::search_space_generator::{FastaSearchSpace, SearchSpaceGenerator};
use macpepdb::mass::to_int as mass_to_int;
use macpepdb::monitoring::{MetricTarget, Monitoring, TracingLogRotation, TracingTarget};
use macpepdb::post_translational_modification::{PTMCollection, PostTranslationalModification};
use macpepdb_tui::Tui;
use reqwest::Url;
use tokio::fs::OpenOptions;
use tracing::{debug, info};

// internal imports
use maccoys::web::results_api::Server as ResultApiServer;

// Allocator
// jemalloc
#[cfg(all(feature = "jemalloc", not(target_env = "msvc")))]
use tikv_jemallocator::Jemalloc;

#[cfg(all(feature = "jemalloc", not(target_env = "msvc")))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

// mimalloc
#[cfg(feature = "mimalloc")]
use mimalloc::MiMalloc;

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

// tcmalloc
#[cfg(feature = "tcmalloc")]
use tcmalloc2::TcMalloc;

#[cfg(feature = "tcmalloc")]
#[global_allocator]
static GLOBAL: TcMalloc = TcMalloc;

static DEFAULT_LOKI_TRACING_LABEL_VALUE: &str = "general";

#[derive(Debug, Subcommand)]
enum WebCommand {
    /// API for accessing the search results
    ResultApi {
        socket: SocketAddr,
        result_dir: PathBuf,
    },
}

/// CLI for the different web APIs
#[derive(Debug, Parser)]
struct WebCLI {
    #[command(subcommand)]
    command: WebCommand,
}

#[derive(Debug, Subcommand)]
enum PipelineCommand {
    /// Prints a new condition to stdout
    NewConfig {},
    /// Runs the full pipline locally.
    ///
    LocalRun {
        /// Storage used by the local pipeline
        #[arg(short, long, value_enum, default_value = "memory")]
        storage: StorageTarget,
        /// Queue used by the local pipeline
        #[arg(short, long, value_enum, default_value = "memory")]
        queue: QueueTarget,
        /// Optional temp folder for intermediate files, default: `<system temp folder>/maccoys`
        #[arg(short, long)]
        tmp_dir: Option<PathBuf>,
        /// PTM file path
        #[arg(short, long)]
        ptms_file: Option<PathBuf>,
        /// Result directroy where each MS run will get a subdirectory
        result_dir: PathBuf,
        /// Path to the configuration file
        config: PathBuf,
        /// Paths to mzML files
        /// Glob patterns are allowed. e.g. /path/to/**/*.mzML, put them in quotes if your shell expands them.
        #[arg(value_delimiter = ' ', num_args = 0..)]
        mzml_file_paths: Vec<String>,
    },
    /// Add the given MS runs to the remote pipeline
    /// Use `search-monitor` after this to monitor the search
    RemoteRun {
        /// PTM file path
        #[arg(short, long)]
        ptms_file: Option<PathBuf>,
        /// Base url to the entrypoin, e.g. http://localhost:8080
        base_url: String,
        /// Search parameters file path, contains `search_parameters`-section of the pipeline configuration
        search_parameters_file: PathBuf,
        /// Paths to mzML files
        /// Glob patterns are allowed. e.g. /path/to/**/*.mzML, put them in quotes if your shell expands them.
        #[arg(value_delimiter = ' ', num_args = 0..)]
        mzml_file_paths: Vec<String>,
    },
    /// Starts a web service which can be made public to submit data to be processed
    RemoteEntrypoint {
        /// Interface where the service is spawned
        interface: String,
        /// Port where the service is spawned
        port: u16,
        /// Contains `index`- & `storages`-section of the pipeline configuration
        config_file_path: PathBuf,
    },
    /// Monitor for remote searches
    SearchMonitor {
        /// Contains `search_parameters`, `index`-, & `storages`-section of the pipeline configuration
        base_url: String,
        /// Search UUID
        uuid: String,
    },
    /// Standalone indexing for distributed processing
    Index {
        /// Path to the indexing configuration file.
        /// Contains `index`-, `preparation`- & `storages`-section of the pipeline configuration
        config_file_path: PathBuf,
    },
    /// Standalone search space generation for distributed processing
    SearchSpaceGeneration {
        /// Path to the indexing configuration file.
        /// Contains `search_space_generation`-, `comet_search`- & `storages`-section of the pipeline configuration
        config_file_path: PathBuf,
    },
    /// Standalone comet search for distributed processing
    Identification {
        /// Optional temp folder for intermediate files, default: `<system temp folder>/maccoys`
        #[arg(short, long)]
        tmp_dir: Option<PathBuf>,
        /// Path to the indexing configuration file.
        /// Contains `comet_search`-, `goodness_and_rescoring`- & `storages`-section of the pipeline configuration
        config_file_path: PathBuf,
    },
    /// Standalone goodness and rescoring for distributed processing
    Scoring {
        /// Path to the indexing configuration file.
        /// Contains `comet_search`-, `goodness_and_rescoring`- & `storages`-section of the pipeline configuration
        config_file_path: PathBuf,
    },
    /// Standalone publication for distributed processing. Should run on the same machine which hosts the file system
    Publication {
        /// Path to the indexing configuration file.
        /// Contains `goodness_and_rescoring`-, `cleanup`- & `storages`-section of the pipeline configuration
        config_file_path: PathBuf,
    },
    /// Standalone error task for distributed processing. Should run on the same machine which hosts the file system
    Error {
        /// Path to the indexing configuration file.
        /// Contains `goodness_and_rescoring`-, `cleanup`- & `storages`-section of the pipeline configuration
        config_file_path: PathBuf,
    },
    /// Http queuing server
    QueueServer {
        /// Directory where queued message payloads are spooled to disk
        data_directory: PathBuf,
        /// Interface where the service is spawned
        interface: String,
        /// Port where the service is spawned
        port: u16,

        config_file_path: PathBuf,
    },
}

#[derive(Debug, Parser)]
struct PipelineCLI {
    #[command(subcommand)]
    command: PipelineCommand,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Starts one of the various web APIs
    Web(WebCLI),
    /// Build search space for the given mass, mass tolerance and PTMs.
    ///
    SearchSpaceBuild {
        // Optional and default arguments
        /// Number of concurrent searches for MultiTask search type
        #[arg(long, default_value_t = NonZeroUsize::new(16).unwrap())]
        concurrent_searches: NonZeroUsize,
        /// URL for decoys targets, can be URL for the database (`scylla://host1,host2,host3/keyspace`) or base url for MaCPepDB web API.
        #[arg(short)]
        decoy_url: Option<String>,
        /// Optional URL for checking generated decoy against targets.
        /// Can be a URL for the database (`scylla://host1,host2,host3/keyspace`),
        /// base url for MaCPepDB web API or base URL to MaCPepDB bloom filters (`bloom+http://<DOMAIN>`).
        /// If not given, decoys will not be checked against the target database and considered as "correct".
        #[arg(short)]
        target_lookup_url: Option<String>,

        // mendatory arguments
        /// Path to search space FASTA file.
        fasta_file_path: String,
        /// Path to PTM file
        ptm_file_path: PathBuf,
        /// Mass
        mass: f64,
        /// Lower mass tolerance in PPM
        lower_mass_tolerance_ppm: i64,
        /// Upper mass tolerance in PPM
        upper_mass_tolerance_ppm: i64,
        /// Maximal number of variable modifications
        max_variable_modifications: usize,
        /// Amount of decoys to generate
        decoys_per_peptide: usize,
        /// URL for fetching targets, can be URL for the database (`postgresq://(<user>(:<password>))@<host>(:<port>)/<database>`) or base url for MaCPepDB web API.
        target_url: String,
    },
    /// Index a spectrum file for fast access. Only mzML is supported yet.
    ///
    IndexSpectrumFile {
        /// Path to spectrum file
        spectrum_file_path: String,
        /// Path to index file
        index_file_path: String,
        /// Chunks to read from file at ones. Increase it if you have a lot on memory. Default: [crate::io::mzml::indexer::DEFAULT_CHUNK_SIZE]
        #[arg(short = 'c')]
        chunks_size: Option<usize>,
    },
    /// Extract a spectrum from a spectrum file into a separate valid file. Only mzML is supported yet.
    ExtractSpectrum {
        /// Path to original spectrum file
        original_spectrum_file_path: String,
        /// Path to index file for the original spectrum file
        index_file_path: String,
        /// Spectrum ID
        spectrum_id: String,
        /// Path to output file
        output_file_path: String,
    },
    /// Prints a new pipeline configuration to stdout
    Config {},
    /// MaCcoyS search pipeline
    Pipeline(PipelineCLI),
}

#[derive(Parser)]
#[command(name = "maccoys")]
struct Cli {
    /// Flag to start tokio console API
    #[arg(long, default_value_t = false)]
    console: bool,
    /// Socket for tokio console API
    #[arg(long)]
    console_socket: Option<SocketAddr>,
    /// Database URL, format `postgresql://[<user>[:<url-safe-password>]@]<host[:<port>]>[,<host[:<port>]>...]/<dbname>[?pool_size=N&...]`
    #[arg(short, long, default_value_t = String::from("postgresql://postgres@127.0.0.1:5432/postgres"))]
    database_url: String,
    /// Path to optional log file
    #[arg(long)]
    log_file: Option<PathBuf>,
    #[arg(long, default_value_t = TracingLogRotation::Daily)]
    log_rotation: TracingLogRotation,
    /// Endpoint for Loki API to send logs to
    #[arg(long)]
    loki: Option<Url>,
    /// Value for the `instance` label used to distinguish logs from this app instance in Loki
    #[arg(long, default_value_t = String::from(env!("CARGO_CRATE_NAME")))]
    loki_label: String,
    /// Socket for prometheus collection endpoint
    #[arg(long)]
    prometheus: Option<SocketAddr>,
    /// Flag to show tracing on the stdout, no tui, no metrics
    #[arg(long, default_value_t = false, conflicts_with = "tui")]
    terminal: bool,
    /// If set
    #[arg(long)]
    metrics_to_logs: Option<u64>,
    /// Flag to show a terminal UI for tracing and metics
    #[arg(long, default_value_t = false, conflicts_with = "terminal")]
    tui: bool,
    /// Increases log level each time it is used. 10 and above will activating level tracing for all crates included.
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,

    #[command(subcommand)]
    command: Commands,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    info!("Welcome to MaCoyS - The `S` is silent!");

    let mut tracing_targets: Vec<TracingTarget> = Vec::new();
    let mut metric_targets: Vec<MetricTarget> = Vec::new();

    let tui = cli.tui.then(|| {
        let tui = Tui::builder().title(env!("CARGO_CRATE_NAME")).build();
        tracing_targets.push(TracingTarget::Tui(tui.layer()));
        metric_targets.push(MetricTarget::Tui(tui.recorder()));
        tui.run_raw()
    });

    if cli.terminal {
        tracing_targets.push(TracingTarget::Terminal);
    }

    #[cfg(feature = "tokio-console")]
    if cli.console {
        tracing_targets.push(TracingTarget::Console(cli.console_socket));
    }

    if let Some(log_file_path) = cli.log_file {
        tracing_targets.push(TracingTarget::File(
            log_file_path.clone(),
            cli.log_rotation.into(),
        ));
    }

    if let Some(loki_url) = cli.loki {
        let labels = vec![
            ("instance".to_string(), cli.loki_label.clone()),
            (
                "pipeline_stage".to_string(),
                pipeline_stage_label(&cli.command).to_string(),
            ),
        ];
        tracing_targets.push(TracingTarget::Loki(loki_url, labels));
    }

    if let Some(prometheus_socket) = cli.prometheus {
        metric_targets.push(MetricTarget::Prometheus(
            prometheus_socket,
            Box::pin(shutdown_signal()),
        ));
    }

    if let Some(metrics_to_logs_period) = cli.metrics_to_logs {
        metric_targets.push(MetricTarget::Tracing(metrics_to_logs_period));
    }

    let _monitoring = Monitoring::new(
        cli.verbose,
        tracing_targets.into_iter(),
        metric_targets.into_iter(),
        HashMap::new(),
    )
    .await
    .unwrap();

    match cli.command {
        Commands::Web(web_command) => match web_command.command {
            WebCommand::ResultApi { socket, result_dir } => {
                ResultApiServer::start(socket, result_dir, Box::pin(shutdown_signal())).await?;
            }
        },
        Commands::SearchSpaceBuild {
            concurrent_searches,
            decoy_url,
            target_lookup_url,
            fasta_file_path,
            ptm_file_path,
            mass,
            lower_mass_tolerance_ppm,
            upper_mass_tolerance_ppm,
            max_variable_modifications,
            decoys_per_peptide,
            target_url,
        } => {
            let ptms = csv::ReaderBuilder::new()
                .delimiter(b'\t')
                .has_headers(true)
                .from_path(ptm_file_path)
                .unwrap()
                .deserialize()
                .map(|result| result.map(Arc::new))
                .collect::<Result<Vec<Arc<PostTranslationalModification>>, csv::Error>>()
                .unwrap();

            let ptms = PTMCollection::new(ptms).map(Arc::new).unwrap();

            let fasta_file = OpenOptions::new()
                .read(true)
                .write(true)
                .truncate(true)
                .append(false)
                .open(fasta_file_path)
                .await
                .context("Error when opening FASTA file")?;
            let fasta_file = Box::pin(fasta_file);

            let mut search_space = FastaSearchSpace::new(fasta_file);

            let search_space_generator =
                SearchSpaceGenerator::new(&target_url, decoy_url, target_lookup_url).await?;

            search_space_generator
                .create(
                    &mut search_space,
                    mass_to_int(mass),
                    lower_mass_tolerance_ppm,
                    upper_mass_tolerance_ppm,
                    max_variable_modifications,
                    ptms,
                    decoys_per_peptide,
                    concurrent_searches,
                )
                .await?;
        }
        Commands::IndexSpectrumFile {
            spectrum_file_path,
            index_file_path,
            chunks_size,
        } => {
            let mzml_path = Path::new(&spectrum_file_path);
            let mut mzml_bytes_reader = BufReader::new(File::open(mzml_path)?);
            let mut mzml_file =
                MzmlReader::read_indexed(&mut mzml_bytes_reader, chunks_size, false, false)?;
            let mut spectrum_offsets = mzml_file.get_index().get_spectra().clone();

            let spectrum_ids = spectrum_offsets.keys().cloned().collect::<Vec<_>>();

            for spec_id in spectrum_ids {
                let spectrum = mzml_file
                    .get_spectrum(&spec_id)
                    .context("Getting spectrum")?;

                let ms_level: u8 = spectrum.get_ms_level().unwrap_or(0);

                if ms_level == 2 {
                    spectrum_offsets.remove(&spec_id);
                }
            }
            let ms2_filtered_index = Index::new(
                spectrum_offsets,
                mzml_file.get_index().get_chromatograms().clone(),
            );
            write_file(
                Path::new(&index_file_path),
                ms2_filtered_index.to_json()?.as_bytes(),
            )?;
        }
        Commands::ExtractSpectrum {
            original_spectrum_file_path,
            index_file_path,
            spectrum_id,
            output_file_path,
        } => {
            let index = Index::from_json(&read_to_string(Path::new(&index_file_path))?)?;
            let mzml_path = Path::new(&original_spectrum_file_path);
            let mut mzml_bytes_reader = BufReader::new(File::open(mzml_path)?);
            let mut mzml_file =
                MzmlReader::read_pre_indexed(&mut mzml_bytes_reader, index, None, false)?;
            write_file(
                Path::new(&output_file_path),
                mzml_file.extract_spectrum(&spectrum_id, true)?,
            )?;
        }
        Commands::Config {} => {
            let new_config = PipelineConfiguration::default();
            println!("{}", toml::to_string_pretty(&new_config)?);
        }
        Commands::Pipeline(pipeline_command) => match pipeline_command.command {
            PipelineCommand::NewConfig {} => {
                let new_config = PipelineConfiguration::new();
                println!("{}", toml::to_string_pretty(&new_config)?);
            }
            PipelineCommand::LocalRun {
                queue,
                storage,
                tmp_dir,
                ptms_file,
                result_dir,
                config,
                mzml_file_paths,
            } => {
                let tmp_dir = match tmp_dir {
                    Some(tmp_dir) => tmp_dir,
                    None => env::temp_dir().join("maccoys"),
                };

                debug!("Temp folder: {}", tmp_dir.display());

                let config: PipelineConfiguration =
                    toml::from_str(&read_to_string(&config).context("Reading config file")?)
                        .context("Deserialize config")?;

                let ptms = ptms_file
                    .map(|path| {
                        csv::ReaderBuilder::new()
                            .delimiter(b'\t')
                            .has_headers(true)
                            .from_path(path)
                            .unwrap()
                            .deserialize()
                            .map(|result| result.map(Arc::new))
                            .collect::<Result<Vec<Arc<PostTranslationalModification>>, csv::Error>>(
                            )
                            .unwrap()
                    })
                    .unwrap_or_default();

                let mzml_file_paths = convert_str_paths_and_resolve_globs(mzml_file_paths)?;

                let search_params = config.search_parameters.clone();

                build_and_run_local_pipeline(
                    queue,
                    storage,
                    result_dir,
                    tmp_dir,
                    config,
                    search_params,
                    ptms,
                    mzml_file_paths,
                    &tui,
                )
                .await?
            }
            PipelineCommand::RemoteRun {
                ptms_file,
                base_url,
                search_parameters_file,
                mzml_file_paths,
            } => {
                let mzml_file_paths = convert_str_paths_and_resolve_globs(mzml_file_paths)?;

                info!("Enqueue into remote pipeline");
                let uuid = RemotePipeline::run(
                    base_url,
                    search_parameters_file,
                    mzml_file_paths,
                    ptms_file,
                )
                .await?;
                tracing::info!("Do not forget your UUID: {}", uuid);
            }
            PipelineCommand::RemoteEntrypoint {
                interface,
                port,
                config_file_path,
            } => {
                let config: RemoteEntypointConfiguration = toml::from_str(
                    &read_to_string(&config_file_path).context("Reading config file")?,
                )
                .context("Deserialize config")?;

                RemotePipelineWebApi::start(interface, port, config).await?;
            }
            PipelineCommand::SearchMonitor { base_url, uuid } => {
                info!("Running search monitor");
                RemotePipeline::start_remote_search_monitor(base_url, &uuid).await?;
            }
            // PipelineCommand::QueueMonitor { base_url } => {
            //     info!("Running queue monitor");
            //     Pipeline::start_remote_queue_monitor(base_url).await?;
            // }
            // TODO: Is there a more generic way to implment the standalone tasks?
            // Only thing which changes is the configuration type and config attributes to call for the input and output queues
            // Except the cleanup task, which does not have an output queue
            PipelineCommand::Index { config_file_path } => {
                IndexingTask::run_standalone(config_file_path).await?
            }
            PipelineCommand::SearchSpaceGeneration { config_file_path } => {
                SearchSpaceGenerationTask::run_standalone(config_file_path).await?
            }
            PipelineCommand::Identification {
                tmp_dir,
                config_file_path,
            } => {
                let tmp_dir = match tmp_dir {
                    Some(tmp_dir) => tmp_dir,
                    None => env::temp_dir().join("maccoys"),
                };
                IdentificationTask::run_standalone(tmp_dir, config_file_path).await?
            }
            PipelineCommand::Scoring { config_file_path } => {
                ScoringTask::run_standalone(config_file_path).await?
            }
            PipelineCommand::Publication { config_file_path } => {
                PublicationTask::run_standalone(config_file_path).await?
            }
            PipelineCommand::Error { config_file_path } => {
                ErrorTask::run_standalone(config_file_path).await?
            }
            PipelineCommand::QueueServer {
                interface,
                port,
                data_directory,
                config_file_path,
            } => {
                let config: RemoteEntypointConfiguration = toml::from_str(
                    &fs::read_to_string(&config_file_path).context("Reading config file")?,
                )
                .context("Deserialize config")?;
                QueueServer::run_standalone(interface, port, config, data_directory).await?;
            }
        },
    };
    Ok(())
}

/// Converts a vector of strings to a vector of paths and resolves glob patterns.
///
/// # Arguments
/// * `paths` - Vector of paths as strings
///
fn convert_str_paths_and_resolve_globs(paths: Vec<String>) -> Result<Vec<PathBuf>> {
    Ok(paths
        .into_iter()
        .map(|path| {
            if !path.contains("*") {
                // Return plain path in vecotor if no glob pattern is found
                Ok(vec![Path::new(&path).to_path_buf()])
            } else {
                // Resolve glob pattern and return array of paths
                Ok(glob(&path)?
                    .map(|x| Ok(x?))
                    .collect::<Result<Vec<PathBuf>>>()?)
            }
        })
        .collect::<Result<Vec<_>>>()? // Collect and resolve errors from parsing/resolving
        .into_iter()
        .flatten() // flatten the vectors which
        .collect())
}

fn pipeline_stage_label(commands: &Commands) -> &'static str {
    match commands {
        Commands::Web(web_command) => match &web_command.command {
            WebCommand::ResultApi {
                socket: _,
                result_dir: _,
            } => ResultApiServer::loki_tracing_label_value(),
        },
        Commands::Pipeline(pipeline_command) => match &pipeline_command.command {
            PipelineCommand::RemoteEntrypoint {
                interface: _,
                port: _,
                config_file_path: _,
            } => RemotePipelineWebApi::loki_tracing_label_value(),
            PipelineCommand::Index {
                config_file_path: _,
            } => indexing_task::loki_tracing_label_value(),
            PipelineCommand::SearchSpaceGeneration {
                config_file_path: _,
            } => search_space_generation_task::loki_tracing_label_value(),
            PipelineCommand::Identification {
                tmp_dir: _,
                config_file_path: _,
            } => identification_task::loki_tracing_label_value(),
            PipelineCommand::Scoring {
                config_file_path: _,
            } => scoring_task::loki_tracing_label_value(),
            PipelineCommand::Publication {
                config_file_path: _,
            } => publication_task::loki_tracing_label_value(),
            PipelineCommand::Error {
                config_file_path: _,
            } => error_task::loki_tracing_label_value(),
            PipelineCommand::QueueServer {
                interface: _,
                port: _,
                data_directory: _,
                config_file_path: _,
            } => QueueServer::loki_tracing_label_value(),
            _ => DEFAULT_LOKI_TRACING_LABEL_VALUE,
        },
        _ => DEFAULT_LOKI_TRACING_LABEL_VALUE,
    }
}

/// Axum shutdown signal handler for ctrl-c and terminate signals
///
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
