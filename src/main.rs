use anyhow::Result;
use std::collections::HashMap;
use std::path::Path;
use tokio::fs::File;
use tokio::io;
use tokio_stream::StreamExt;

#[derive(Clone, Debug, serde::Deserialize, PartialEq)]
struct AccountInfo {
    available: f32, // Available for use.
    held: f32,      // Held because of a disputed charge.
    locked: bool,   // Has been locked because of a chargeback.
}

impl Default for AccountInfo {
    fn default() -> Self {
        AccountInfo {
            available: 0.0,
            held: 0.0,
            locked: false,
        }
    }
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize, PartialEq)]
struct AccountInfoWithTotal {
    client: u16,
    available: f32,
    held: f32,
    total: f32,
    locked: bool,
}

#[derive(Clone, Debug, serde::Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
enum TransactionType {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    Chargeback,
}

#[derive(Clone, Debug, serde::Deserialize, PartialEq)]
struct LedgerEntry {
    #[serde(rename(serialize = "type", deserialize = "type"))]
    tx_type: TransactionType, // Transaction Type. Will be with header name 'type', which is unfortunately a reserved word in rust.
    client: u16,         // Client ID
    tx: u32,             // Transaction ID
    amount: Option<f32>, // At least four decimal places of precision is expected. Not present on every type of transaction.
    #[serde(skip)]
    #[serde(default = "disputed_default")]
    is_disputed: bool,
}

fn disputed_default() -> bool {
    false
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize, PartialEq)]
#[serde(rename_all = "lowercase")]
#[serde(tag = "type")]
enum LedgerEntryEnum {
    Deposit { client: u16, tx: u32, amount: f32 },
    Withdrawal { client: u16, tx: u32, amount: f32 },
    Dispute { client: u16, tx: u32 },
    Resolve { client: u16, tx: u32 },
    Chargeback { client: u16, tx: u32 },
}

enum TransactionError {
    DuplicateTransactionId,
    MissingAmountField,
    CannotWithdrawFromNonexistantClient,
    CannotOverdrawByWithdrawal,
    CannotDisputeNonexistantTransaction,
    CannotDisputeTransactionForNonexistantCustomer,
    CannotDisputeAlreadyDisputedTransaction,
    CannotResolveNonexistantTransaction,
    CannotResolveTransactionForNonexistantCustomer,
    CannotResolveUndisputedTransaction,
    CannotChargebackNonexistantTransaction,
    CannotChargebackTransactionForNonexistantCustomer,
    CannotChargebackUndisputedTransaction,
    CannotDepoistIntoLockedAccount,
    CannotWithdrawFromLockedAccount,
}

#[tokio::main]
pub async fn main() -> Result<()> {
    eprintln!("Hello, world!");

    let args: Vec<_> = std::env::args().collect();
    let filename = match args.get(2) {
        None => {
            // eprintln!("Require filename to read in as an argument.");
            // std::process::exit(-1);
            // Assume that the default is "transactions.csv"
            "transactions.csv".to_string()
        }
        Some(file_name) => file_name.to_string(),
    };

    eprintln!("Filename: {filename}");

    let mut ledger = process_transactions(filename).await?;
    eprintln!("Resulting ledger length: {}", ledger.len());
    write_ledger(&mut ledger).await?;

    Ok(())
}

async fn write_ledger(ledger: &mut HashMap<u16, AccountInfo>) -> Result<()> {
    let mut wri = csv_async::AsyncWriterBuilder::new()
        .has_headers(true)
        .create_serializer(io::stdout());

    for entry in ledger.into_iter().map(|(client_id, account_info)| {
        convert_account_info_to_account_info_with_total(account_info, client_id)
    }) {
        wri.serialize(entry).await?;
    }

    Ok(())
}

fn transaction_id_is_new(txid: &u32, transaction_ledger: &HashMap<u32, LedgerEntry>) -> bool {
    !transaction_ledger.contains_key(txid)
}

async fn process_transactions(filename: impl AsRef<Path>) -> Result<HashMap<u16, AccountInfo>> {
    let file_handle = File::open(filename).await?;
    let mut result: HashMap<u16, AccountInfo> = HashMap::from([]);
    let mut transaction_ledger: HashMap<u32, LedgerEntry> = HashMap::from([]);
    let mut failed_transactions: Vec<(LedgerEntry, TransactionError)> = vec![];
    let rdr = csv_async::AsyncReaderBuilder::new()
        .flexible(true)
        .trim(csv_async::Trim::All)
        .create_deserializer(file_handle);
    let mut records = rdr.into_deserialize::<LedgerEntry>();

    while let Some(record) = records.next().await {
        let record: LedgerEntry = record?;
        process_transaction(
            &record,
            &mut result,
            &mut transaction_ledger,
            &mut failed_transactions,
        );
    }

    Ok(result)
}

fn process_transaction(
    record: &LedgerEntry,
    result: &mut HashMap<u16, AccountInfo>,
    transaction_ledger: &mut HashMap<u32, LedgerEntry>,
    failed_transactions: &mut Vec<(LedgerEntry, TransactionError)>,
) {
    match record.tx_type {
        TransactionType::Deposit => {
            eprintln!("Found a deposit.");
            if !transaction_id_is_new(&record.tx, &transaction_ledger) {
                // We've encountered an invalid transaction ID. Assume
                // that we should refuse to process the transaction
                // and continue on.
                eprintln!("Deposit: Duplicate transaction ID.");
                failed_transactions
                    .push((record.clone(), TransactionError::DuplicateTransactionId));
                return;
            }
            if record.amount.is_none() {
                eprintln!("Deposit: Missing amount field.");
                failed_transactions.push((record.clone(), TransactionError::MissingAmountField));
                return;
            }
            match result.get(&record.client) {
                None => (),
                Some(client_info) => {
                    if client_info.locked {
                        failed_transactions.push((
                            record.clone(),
                            TransactionError::CannotDepoistIntoLockedAccount,
                        ));
                    }
                }
            }
            eprintln!("\tInserting deposit.");

            result
                .entry(record.client)
                .and_modify(|client_data| client_data.available += record.amount.unwrap())
                .or_insert(AccountInfo {
                    available: record.amount.unwrap(),
                    ..Default::default()
                });
            transaction_ledger.insert(record.tx, record.clone());
        }
        TransactionType::Withdrawal => {
            eprintln!("Found a Withdrawal.");
            if !transaction_id_is_new(&record.tx, &transaction_ledger) {
                // We've encountered an invalid transaction ID. Assume that we should refuse to process the transaction and continue on.
                eprintln!("Withdrawal: Duplicate transaction ID.");
                failed_transactions
                    .push((record.clone(), TransactionError::DuplicateTransactionId));
            }
            if record.amount.is_none() {
                eprintln!("Withdrawal: Missing Amount Field.");
                failed_transactions.push((record.clone(), TransactionError::MissingAmountField));
                return;
            }

            if !result.contains_key(&record.client) {
                eprintln!("Withdrawal: Cannot Withdraw from Nonexistant Client.");
                failed_transactions.push((
                    record.clone(),
                    TransactionError::CannotWithdrawFromNonexistantClient,
                ));
                return;
            }
            if result.get(&record.client).unwrap().locked {
                failed_transactions.push((
                    record.clone(),
                    TransactionError::CannotWithdrawFromLockedAccount,
                ));
                return;
            }
            if result.get(&record.client).unwrap().available < record.amount.unwrap() {
                failed_transactions
                    .push((record.clone(), TransactionError::CannotOverdrawByWithdrawal));
                return;
            }

            result
                .entry(record.client)
                .and_modify(|client_data| client_data.available -= record.amount.unwrap());
            transaction_ledger.insert(record.tx, record.clone());
        }
        TransactionType::Dispute => {
            if !result.contains_key(&record.client) {
                failed_transactions.push((
                    record.clone(),
                    TransactionError::CannotDisputeTransactionForNonexistantCustomer,
                ));
            }
            if transaction_id_is_new(&record.tx, &transaction_ledger) {
                // We're trying to dispute a non-existant transaction.
                failed_transactions.push((
                    record.clone(),
                    TransactionError::CannotDisputeNonexistantTransaction,
                ));
                return;
            }
            if transaction_ledger.get(&record.tx).unwrap().is_disputed {
                failed_transactions.push((
                    record.clone(),
                    TransactionError::CannotDisputeAlreadyDisputedTransaction,
                ));
                return;
            }
            let transaction_amount = get_transaction_amount(&record.tx, &transaction_ledger);
            result.entry(record.client).and_modify(|client_data| {
                client_data.available -= transaction_amount;
                client_data.held += transaction_amount;
            });
        }
        TransactionType::Resolve => {
            if !result.contains_key(&record.client) {
                failed_transactions.push((
                    record.clone(),
                    TransactionError::CannotResolveTransactionForNonexistantCustomer,
                ));
            }
            if transaction_id_is_new(&record.tx, &transaction_ledger) {
                // We're trying to resolve a non-existant transaction.
                failed_transactions.push((
                    record.clone(),
                    TransactionError::CannotResolveNonexistantTransaction,
                ));
                return;
            }
            if !transaction_ledger.get(&record.tx).unwrap().is_disputed {
                failed_transactions.push((
                    record.clone(),
                    TransactionError::CannotResolveUndisputedTransaction,
                ));
                return;
            }
            let transaction_amount = get_transaction_amount(&record.tx, &transaction_ledger);
            result.entry(record.client).and_modify(|client_data| {
                client_data.available += transaction_amount;
                client_data.held -= transaction_amount;
            });
        }
        TransactionType::Chargeback => {
            if transaction_id_is_new(&record.tx, &transaction_ledger) {
                // We're trying to resolve a non-existant transaction.
                failed_transactions.push((
                    record.clone(),
                    TransactionError::CannotChargebackNonexistantTransaction,
                ));
                return;
            }
            if !result.contains_key(&record.client) {
                failed_transactions.push((
                    record.clone(),
                    TransactionError::CannotChargebackTransactionForNonexistantCustomer,
                ));
            }
            if !transaction_ledger.get(&record.tx).unwrap().is_disputed {
                failed_transactions.push((
                    record.clone(),
                    TransactionError::CannotChargebackUndisputedTransaction,
                ));
                return;
            }
            let transaction_amount = get_transaction_amount(&record.tx, &transaction_ledger);
            result.entry(record.client).and_modify(|client_data| {
                client_data.held -= transaction_amount;
                client_data.locked = true;
            });
        }
    }
}

fn get_transaction_amount(tx: &u32, transaction_ledger: &HashMap<u32, LedgerEntry>) -> f32 {
    transaction_ledger.get(tx).unwrap().amount.unwrap()
}

fn convert_account_info_to_account_info_with_total(
    ai: &AccountInfo,
    client: &u16,
) -> AccountInfoWithTotal {
    AccountInfoWithTotal {
        client: *client,
        available: ai.available,
        held: ai.held,
        total: ai.available + ai.held,
        locked: ai.locked,
    }
}
