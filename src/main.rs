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
    let mut server = tokio::spawn(
        run()
    );

    let server_result = tokio::select!(
        res = &mut server => {
            match res {
                Ok(Ok(())) => Ok(()),
                Ok(Err(e)) => Err(e),
                Err(e) => Err(e.into()),
            }
        }
        _ = tokio::signal::ctrl_c() => {
            println!("----- Shutdown signal received -----");
            Ok(())
        }
    );
    
    server_result
}