- thought about why use tokio::spawn vs td::thread::spawn ? 
    - since health checker is mostly check & sleep, shoudl put it in async task
    - if cpu heavy task -> then might use thread::spawn
    - dont wanna stall other async tasks 

- not gonna use try_join (wait for all)
    - chances of stalling 
    - starvation risk

- use of select! 
        - network fails/stops 
                                - select! → main handles it
        - shutdown requested

- backend pool
    - health is atomic boolean flag
        - to avoid intermediate garbage value

- Error reporting
    - anyhow for high level application error Result<Config, anyhow::Error>
    - for low networking level
        - sticking with td::io::Result<()>