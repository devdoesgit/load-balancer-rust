mod network;

use network::connection::run;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // load configs
    // let configs = Config::load_config();

    // load active backend modules
    // let backend_pool = BackendPool::new(configs);

    // start health-checker
    // health::start();

    // initalize load-balancer and bind port
    tokio::try_join!(
        run(),
    )?;
    
    Ok(())
}