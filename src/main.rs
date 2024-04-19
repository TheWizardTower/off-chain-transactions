use either::{Either, Left, Right};
use std::collections::HashMap;
use tokio::fs::File;
use tokio_stream::StreamExt;

// Some convenience types.
pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;

enum TransactionError {
    MissingSource,
    MissingDest,
    Overdraft,
    MissingSourceAndDest,
}

#[derive(Clone, Debug, serde::Deserialize)]
struct LedgerEntry {
    source: String,
    dest: String,
    amount: u32,
}

#[derive(Debug, serde::Deserialize)]
struct Balance {
    name: String,
    amount: i64,
}

#[tokio::main]
pub async fn main() -> Result<()> {
    println!("Hello, world!");

    let mut ledger = accounts_init("init.csv".to_string())
        .await
        .expect("Could not read initial file.");

    process_records(&mut ledger, "offchain_transactions.csv".to_string())
        .await
        .expect("Could not process transactions");

    println!("\n\nFinal balance:");
    for (name, bal) in ledger {
        println!("\t{name}: {bal}");
    }

    Ok(())
}

pub async fn accounts_init(filename: String) -> Result<HashMap<String, i64>> {
    let mut result = HashMap::new();

    let file_handle = File::open(filename).await?;
    let mut rdr = csv_async::AsyncDeserializer::from_reader(file_handle);
    let mut records = rdr.deserialize::<Balance>();

    while let Some(record) = records.next().await {
        let record: Balance = record?;
        println!("{:?}", record);
        result.insert(record.name, record.amount);
    }
    println!("\n\n");

    Ok(result)
}

pub async fn process_records(
    ledger: &mut HashMap<String, i64>,
    filename: String,
) -> Result<Either<Vec<(LedgerEntry, TransactionError)>, ()>> {
    let file_handle = File::open(filename).await?;
    let mut rdr = csv_async::AsyncDeserializer::from_reader(file_handle);
    let mut records = rdr.deserialize::<LedgerEntry>();
    let mut failed_transactions: Vec<(LedgerEntry, TransactionError)> = vec![];

    while let Some(record) = records.next().await {
        let record: LedgerEntry = record?;
        println!("{:?}", record);

        let record_amount: i64 = record.amount.into();
        match (
            ledger.contains_key(&record.source),
            ledger.contains_key(&record.dest),
        ) {
            (true, true) => {
                {
                    let src_bal = ledger.get_mut(&record.source).unwrap();
                    if *src_bal <= 0 {
                        println!("Denyiing transaction, source balance is already negative.");
                        failed_transactions.push((record.clone(), TransactionError::Overdraft));
                    } else if *src_bal - record_amount < 0 {
                        println!("Overdraft, adding a fee to the deduction.");
                        *src_bal -= record_amount + 35;
                    } else {
                        *src_bal -= record_amount;
                    }
                }
                {
                    let dest_bal = ledger.get_mut(&record.dest).unwrap();
                    *dest_bal += record_amount;
                }
            }
            (false, true) => {
                println!(
                    "Source of transaction ({}) was not found in ledger, aborting.",
                    record.source
                );
                failed_transactions.push((record.clone(), TransactionError::MissingSource))
            }
            (true, false) => {
                println!(
                    "Destination of transaction ({}) was not found in ledger, aborting.",
                    record.source
                );
                failed_transactions.push((record.clone(), TransactionError::MissingDest));
            }
            (false, false) => {
                println!(
                    "Neither source ({}) nor dest ({}) were found in ledger, aborting.",
                    record.source, record.dest
                );
                failed_transactions.push((record.clone(), TransactionError::MissingSourceAndDest));
            }
        }
    }

    if failed_transactions.len() == 0 {
        return Ok(Right(()));
    }
    Ok(Left(failed_transactions))
}
