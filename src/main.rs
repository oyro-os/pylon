//! pylon binary entrypoint.
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    pylon::init_tracing();
    tracing::info!("pylon starting (scaffold — wiring completed in Task 14)");
    Ok(())
}
