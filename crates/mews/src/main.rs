#[tokio::main]
async fn main() -> std::process::ExitCode {
    match mews::cli::run().await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            if let Some(error) = error.downcast_ref::<mews::cli::RestartFailure>() {
                eprintln!("{error}");
            } else {
                eprintln!("Error: {error:#}");
            }
            std::process::ExitCode::FAILURE
        }
    }
}
