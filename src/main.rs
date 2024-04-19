use std::collections::HashMap;
use tokio::fs::File;
use tokio_stream::StreamExt;

pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// A specialized `Result` type for mini-redis operations.
///
/// This is defined as a convenience.
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, serde::Deserialize)]
struct LedgerEntry {
    source: String,
    dest: String,
    amount: i64,
}

struct Balance {
    name: String,
    amount: i64,
}

#[tokio::main]
pub async fn main() -> Result<()> {
    println!("Hello, world!");

    let mut ledger = accounts_init();
    let filename = "offchain_transactions.csv";

    let file_handle = File::open(filename).await?;
    let mut rdr = csv_async::AsyncDeserializer::from_reader(file_handle);
    let mut records = rdr.deserialize::<LedgerEntry>();

    while let Some(record) = records.next().await {
        let record: LedgerEntry = record?;
        println!("{:?}", record);

        match (
            ledger.contains_key(&record.source),
            ledger.contains_key(&record.dest),
        ) {
            (true, true) => {
                {
                    let src_bal = ledger.get_mut(&record.source).unwrap();
                    *src_bal -= record.amount;
                }
                {
                    let dest_bal = ledger.get_mut(&record.dest).unwrap();
                    *dest_bal += record.amount;
                }
            }
            (false, true) => {
                println!(
                    "Source of transaction ({}) was not found in ledger, aborting.",
                    record.source
                );
            }
            (true, false) => {
                println!(
                    "Destination of transaction ({}) was not found in ledger, aborting.",
                    record.source
                );
            }
            (false, false) => {
                println!(
                    "Neither source ({}) nor dest ({}) were found in ledger, aborting.",
                    record.source, record.dest
                );
            }
        }
    }

    Ok(())
}

pub fn accounts_init() -> HashMap<String, i64> {
    HashMap::from([
        ("Chain".to_string(), 2_000_000),
        ("Adam".to_string(), 1_000),
        ("Michael".to_string(), 2_000),
        ("Brewery".to_string(), 5_000),
        ("Sushi To Go".to_string(), 10_000),
        ("Board Game Store".to_string(), 3_000),
    ])
}
