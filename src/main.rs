use anyhow::Result;
use log::{error, info};
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;
use std::collections::HashMap;

mod processing;
mod render_ledger;
mod types;

#[cfg(test)]
mod tests;

use crate::processing::process_transactions;
use crate::render_ledger::write_ledger;
use crate::types::*;

#[tokio::main]
pub async fn main() -> Result<()> {
    env_logger::init();

    info!("Hello, world!");

    let args: Vec<_> = std::env::args().collect();
    let filename = match args.get(1) {
        None => {
            // We also could return a default value, which may make
            // in-editor calls to `cargo test` a bit more convenient,
            // but that isn't a good 'default' option.
            error!("Require filename to read in as an argument.");
            std::process::exit(-1);
            // Assume that the default is "transactions.csv"
            // "transactions.csv".to_string()
        }
        // the to_string() call is only there to get rid of the borrow.
        Some(file_name) => file_name.to_string(),
    };

    info!("Filename: {filename}");

    let mut ledger = process_transactions(filename).await?;
    info!("Resulting ledger length: {}", ledger.len());
    write_ledger(&mut ledger).await?;

    Ok(())
}
