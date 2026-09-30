mod network;
mod health;
mod config;

use config::Config;

use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // load configs
    let configs = match Config::load() {
        Ok(c) => {
            println!("Config Loaded...");
            println!("{}", c);
            c
        },
        Err(e) => {
            // return immediately
            return Err(e);
        }
    };

    // load active backend modules
    // let backend_pool = BackendPool::new(configs);

    // create cancellation token
    let token = CancellationToken::new();
    let health_token = token.clone();
    let health_interval = std::time::Duration::from_secs(configs.health.interval_seconds.max(1));

    // start health-checker
    let health = tokio::spawn(
        async move{
            loop {
                let res = health::health_checker::start(health_token.clone(), health_interval).await;
                match res {
                    Ok(()) => {
                        println!("\n!----- Shutting Down Health Checker -----!");
                        break
                    },
                    Err(e) => eprintln!("Health checker exited with error: {:?}", e)
                }
                // sleep -> restart every 10 seconds
                tokio::time::sleep(std::time::Duration::from_secs(10)).await;
            }
        }
    );
    
    let server_result = tokio::select!(
        res = network::connection::run(token.clone()) => {
            res
        }
        signal = tokio::signal::ctrl_c() => {
            signal?;
            println!("!----- Shutting Down Load Balancer -----!\n");
            Ok(())
        }
    );    
    
    token.cancel();
    health.await?;

    server_result?;
    Ok(())
}