use anyhow::Result;
use log::{debug, info};
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;
use std::collections::HashMap;
use std::path::Path;
use tokio::fs::File;
use tokio::io;
use tokio_stream::StreamExt;

// Struct for storing account information as we process transactions.
// We choose *not* to record Total, as keeping track of that is
// inviting opportunities for both rounding error (mitigated by the
// Decimal type) and more importantly, is the sort of thing you'd
// forget to do as you're refactoring, bugfixing, or extending code.
// So, track available and held, and then when we render the ledger at
// the end, calculate total as available + held.
//
// Additionally, we're storing this in a HashMap of u16 to
// AccountInfo. The u16 of course represents the customer ID, and
// putting it in a hash map encodes an invariant (every customer shall
// have only one account) into our data structures nicely. It's also
// something we can easily re-render into an AccountInfoWithTotal at
// the end of processing.
#[derive(Clone, Debug, serde::Deserialize, PartialEq)]
struct AccountInfo {
    available: Decimal, // Available for use.
    held: Decimal,      // Held because of a disputed charge.
    locked: bool,       // Has been locked because of a chargeback.
}

impl Default for AccountInfo {
    fn default() -> Self {
        AccountInfo {
            available: dec!(0.0),
            held: dec!(0.0),
            locked: false,
        }
    }
}

// We use this type to render nice CSV via serde and csv_async at the
// end. We're using the above struct for in-flight bookkeeping.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize, PartialEq)]
struct AccountInfoWithTotal {
    client: u16,
    available: Decimal,
    held: Decimal,
    total: Decimal,
    locked: bool,
}

// Create a sum type for the types of transactions we can process.
// We're also doing some serde footwork to make it so we get nice
// Rust-format names while still accepting 'deposit' and so forth in
// the actual CSV files.
#[derive(Clone, Debug, serde::Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
enum TransactionType {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    Chargeback,
}

#[derive(Clone, Debug, PartialEq)]
enum TransactionStatus {
    Undisputed,
    Disputed,
    Chargebacked,
}

// The struct we serialize transaction rows into. We have to massage
// this a fair bit to have it line up with the CSV we slurp in, and we
// change it a bit to support recording if a transaction has been
// disputed or not.
//
// First off, we have to rename the `type` field to tx_type, since
// type is of course a reserved word in Rust, so it can't be a struct
// field name. Second, we add an is_disputed field that is not present
// in the CSV, so we can update which transactions have been disputed,
// resolved (i.e., back to 'undisputed'), or chargebacked. If a
// transaction has been chargebacked, it stays there permanently, it
// is not subject to further dispute or resolution.
#[derive(Clone, Debug, serde::Deserialize, PartialEq)]
struct LedgerEntry {
    #[serde(rename(serialize = "type", deserialize = "type"))]
    tx_type: TransactionType, // Transaction Type. Will be with header name 'type', which is unfortunately a reserved word in rust.
    client: u16,             // Client ID
    tx: u32,                 // Transaction ID
    amount: Option<Decimal>, // At least four decimal places of precision is expected. Not present on every type of transaction.
    #[serde(skip)]
    #[serde(default = "disputed_default")]
    disputed_status: TransactionStatus,
}

fn disputed_default() -> TransactionStatus {
    TransactionStatus::Undisputed
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize, PartialEq)]
#[serde(rename_all = "lowercase")]
#[serde(tag = "type")]
enum LedgerEntryEnum {
    Deposit {
        client: u16,
        tx: u32,
        amount: Decimal,
    },
    Withdrawal {
        client: u16,
        tx: u32,
        amount: Decimal,
    },
    Dispute {
        client: u16,
        tx: u32,
    },
    Resolve {
        client: u16,
        tx: u32,
    },
    Chargeback {
        client: u16,
        tx: u32,
    },
}

#[derive(Debug, PartialEq)]
enum TransactionError {
    DuplicateTransactionId,
    MissingAmountField,
    CannotWithdrawFromNonexistantCustomer,
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
    DisputeTransactionClientIDDoesNotMatchRequestClientID,
    ResolveTransactionClientIDDoesNotMatchRequestClientID,
    ChargebackTransactionClientIDDoesNotMatchRequestClientID,
    CannotChargebackAlreadyChargebackedTransaction,
    CannotResolveChargebackedTransaction,
    CannotDisputeAlreadyChargebackedTransaction,
}

#[tokio::main]
pub async fn main() -> Result<()> {
    env_logger::init();

    info!("Hello, world!");

    let args: Vec<_> = std::env::args().collect();
    let filename = match args.get(1) {
        None => {
            // error!("Require filename to read in as an argument.");
            // std::process::exit(-1);
            // Assume that the default is "transactions.csv"
            "transactions.csv".to_string()
        }
        Some(file_name) => file_name.to_string(),
    };

    info!("Filename: {filename}");

    let mut ledger = process_transactions(filename).await?;
    info!("Resulting ledger length: {}", ledger.len());
    write_ledger(&mut ledger).await?;

    Ok(())
}

async fn write_ledger(ledger: &mut HashMap<u16, AccountInfo>) -> Result<()> {
    let mut wri = csv_async::AsyncWriterBuilder::new()
        .has_headers(true)
        .create_serializer(io::stdout());

    for entry in ledger.iter_mut().map(|(client_id, account_info)| {
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
            process_deposit(record, result, transaction_ledger, failed_transactions);
        }
        TransactionType::Withdrawal => {
            process_withdrawal(record, result, transaction_ledger, failed_transactions);
        }
        TransactionType::Dispute => {
            process_dispute(record, result, transaction_ledger, failed_transactions);
        }
        TransactionType::Resolve => {
            process_resolve(record, result, transaction_ledger, failed_transactions);
        }
        TransactionType::Chargeback => {
            process_chargeback(record, result, transaction_ledger, failed_transactions);
        }
    }
}

fn process_deposit(
    record: &LedgerEntry,
    result: &mut HashMap<u16, AccountInfo>,
    transaction_ledger: &mut HashMap<u32, LedgerEntry>,
    failed_transactions: &mut Vec<(LedgerEntry, TransactionError)>,
) {
    debug!("Found a deposit.");
    if !transaction_id_is_new(&record.tx, transaction_ledger) {
        // We've encountered an invalid transaction ID. Assume
        // that we should refuse to process the transaction
        // and continue on.
        info!("Deposit: Duplicate transaction ID.");
        failed_transactions.push((record.clone(), TransactionError::DuplicateTransactionId));
        return;
    }
    if record.amount.is_none() {
        info!("Deposit: Missing amount field.");
        failed_transactions.push((record.clone(), TransactionError::MissingAmountField));
        return;
    }
    match result.get(&record.client) {
        None => (),
        Some(client_info) => {
            if client_info.locked {
                info!("Deposit: Cannot Deposit into Locked Account.");
                failed_transactions.push((
                    record.clone(),
                    TransactionError::CannotDepoistIntoLockedAccount,
                ));
                return;
            }
        }
    }
    info!("\tInserting deposit.");

    result
        .entry(record.client)
        .and_modify(|client_data| client_data.available += record.amount.unwrap())
        .or_insert(AccountInfo {
            available: record.amount.unwrap(),
            ..Default::default()
        });
    transaction_ledger.insert(record.tx, record.clone());
}

fn process_withdrawal(
    record: &LedgerEntry,
    result: &mut HashMap<u16, AccountInfo>,
    transaction_ledger: &mut HashMap<u32, LedgerEntry>,
    failed_transactions: &mut Vec<(LedgerEntry, TransactionError)>,
) {
    debug!("Found a Withdrawal.");
    if !transaction_id_is_new(&record.tx, transaction_ledger) {
        // We've encountered an invalid transaction ID. Assume that we should refuse to process the transaction and continue on.
        info!("Withdrawal: Duplicate transaction ID.");
        failed_transactions.push((record.clone(), TransactionError::DuplicateTransactionId));
        return;
    }

    if record.amount.is_none() {
        info!("Withdrawal: Missing Amount Field.");
        failed_transactions.push((record.clone(), TransactionError::MissingAmountField));
        return;
    }

    if !result.contains_key(&record.client) {
        info!("Withdrawal: Cannot Withdraw from Nonexistant Client.");
        failed_transactions.push((
            record.clone(),
            TransactionError::CannotWithdrawFromNonexistantCustomer,
        ));
        return;
    }
    if result.get(&record.client).unwrap().locked {
        info!("Withdrawal: Cannot Withdraw from Locked Account.");
        failed_transactions.push((
            record.clone(),
            TransactionError::CannotWithdrawFromLockedAccount,
        ));
        return;
    }
    if result.get(&record.client).unwrap().available < record.amount.unwrap() {
        info!("Withdrawal: Cannot Overdraft Account.");
        failed_transactions.push((record.clone(), TransactionError::CannotOverdrawByWithdrawal));
        return;
    }

    debug!("Withdraw: Processing transaction.");
    result
        .entry(record.client)
        .and_modify(|client_data| client_data.available -= record.amount.unwrap());
    transaction_ledger.insert(record.tx, record.clone());
}

fn process_dispute(
    record: &LedgerEntry,
    result: &mut HashMap<u16, AccountInfo>,
    transaction_ledger: &mut HashMap<u32, LedgerEntry>,
    failed_transactions: &mut Vec<(LedgerEntry, TransactionError)>,
) {
    // Open questions:
    //
    // * Can you dispute a _dispute_ ?
    //   - My intuition is you shouldn't be able to, but this
    //   really ought to be answered by the stakeholder.
    // * Can you dispute a chargeback or resolution? Probably not.
    //
    // * What happens if the client ID on the dispute message does not match the client ID on the transaction?
    //   - We should probably abort, on the assumption that it's invalid data.
    debug!("Found a Dispute.");
    // We check the customer first because, if the customer
    // does not exist, than the associated transaction
    // probably doesn't, either.
    if !result.contains_key(&record.client) {
        info!("Dispute: Cannot Dispute Transaction for Nonexistant Customer");
        failed_transactions.push((
            record.clone(),
            TransactionError::CannotDisputeTransactionForNonexistantCustomer,
        ));
        return;
    }
    if transaction_id_is_new(&record.tx, transaction_ledger) {
        // We're trying to dispute a non-existant transaction.
        info!("Dispute: Cannot Dispute Non-existant Transaction.");
        failed_transactions.push((
            record.clone(),
            TransactionError::CannotDisputeNonexistantTransaction,
        ));
        return;
    }
    if transaction_ledger.get(&record.tx).unwrap().client != record.client {
        // The client ID on the incoming request does not
        // match the client ID of the transaction it referrs
        // to. Assume that this is a malformed request and
        // refuse to process it.
        info!("Dispute: Transaction Client ID does not match record Client ID.");
        failed_transactions.push((
            record.clone(),
            TransactionError::DisputeTransactionClientIDDoesNotMatchRequestClientID,
        ));
        return;
    }
    if transaction_ledger.get(&record.tx).unwrap().disputed_status == TransactionStatus::Disputed {
        info!("Dispute: Cannot Dispute Already Disputed Transaction.");
        failed_transactions.push((
            record.clone(),
            TransactionError::CannotDisputeAlreadyDisputedTransaction,
        ));
        return;
    }

    if transaction_ledger.get(&record.tx).unwrap().disputed_status
        == TransactionStatus::Chargebacked
    {
        info!("Dispute: Cannot Dispute Already Disputed Transaction.");
        failed_transactions.push((
            record.clone(),
            TransactionError::CannotDisputeAlreadyChargebackedTransaction,
        ));
        return;
    }

    // What should we do in the case of a locked account?
    debug!("Dispute: Processing Dispute.");
    let transaction_amount = get_transaction_amount(&record.tx, transaction_ledger);
    result.entry(record.client).and_modify(|client_data| {
        client_data.available -= transaction_amount;
        client_data.held += transaction_amount;
    });
    transaction_ledger
        .entry(record.tx)
        .and_modify(|entry| entry.disputed_status = TransactionStatus::Disputed);
}

fn process_resolve(
    record: &LedgerEntry,
    result: &mut HashMap<u16, AccountInfo>,
    transaction_ledger: &mut HashMap<u32, LedgerEntry>,
    failed_transactions: &mut Vec<(LedgerEntry, TransactionError)>,
) {
    debug!("Found a Resolve.");
    if !result.contains_key(&record.client) {
        info!("Resolve: Cannot Resolve Transaction for Nonexistant Customer.");
        failed_transactions.push((
            record.clone(),
            TransactionError::CannotResolveTransactionForNonexistantCustomer,
        ));
        return;
    }
    if transaction_id_is_new(&record.tx, transaction_ledger) {
        // We're trying to resolve a non-existant transaction.
        info!("Resolve: Cannot Resolve Non-existant Transaction.");
        failed_transactions.push((
            record.clone(),
            TransactionError::CannotResolveNonexistantTransaction,
        ));
        return;
    }

    if transaction_ledger.get(&record.tx).unwrap().client != record.client {
        // The client ID on the incoming request does not
        // match the client ID of the transaction it referrs
        // to. Assume that this is a malformed request and
        // refuse to process it.
        info!("Resolve: Transaction Client ID does not match record Client ID.");
        failed_transactions.push((
            record.clone(),
            TransactionError::ResolveTransactionClientIDDoesNotMatchRequestClientID,
        ));
        return;
    }

    if transaction_ledger.get(&record.tx).unwrap().disputed_status == TransactionStatus::Undisputed
    {
        info!("Resolve: Cannot Resolve Undisputed Transaction.");
        failed_transactions.push((
            record.clone(),
            TransactionError::CannotResolveUndisputedTransaction,
        ));
        return;
    }

    if transaction_ledger.get(&record.tx).unwrap().disputed_status
        == TransactionStatus::Chargebacked
    {
        info!("Resolve: Cannot Resolve Undisputed Transaction.");
        failed_transactions.push((
            record.clone(),
            TransactionError::CannotResolveChargebackedTransaction,
        ));
        return;
    }

    // What should we do in the case of a locked account?
    let transaction_amount = get_transaction_amount(&record.tx, transaction_ledger);
    result.entry(record.client).and_modify(|client_data| {
        client_data.available += transaction_amount;
        client_data.held -= transaction_amount;
    });
    transaction_ledger
        .entry(record.tx)
        .and_modify(|entry| entry.disputed_status = TransactionStatus::Undisputed);
}

fn process_chargeback(
    record: &LedgerEntry,
    result: &mut HashMap<u16, AccountInfo>,
    transaction_ledger: &mut HashMap<u32, LedgerEntry>,
    failed_transactions: &mut Vec<(LedgerEntry, TransactionError)>,
) {
    debug!("Found a Chargeback.");
    if !result.contains_key(&record.client) {
        info!("Chargeback: Cannot Chargeback Transaction For Non-Existant Customer.");
        failed_transactions.push((
            record.clone(),
            TransactionError::CannotChargebackTransactionForNonexistantCustomer,
        ));
        return;
    }

    if transaction_id_is_new(&record.tx, transaction_ledger) {
        // We're trying to chargeback a non-existant transaction.
        info!("Chargeback: Cannot Chargeback a Non-existant Transaction.");
        failed_transactions.push((
            record.clone(),
            TransactionError::CannotChargebackNonexistantTransaction,
        ));
        return;
    }

    if transaction_ledger.get(&record.tx).unwrap().client != record.client {
        // The client ID on the incoming request does not
        // match the client ID of the transaction it referrs
        // to. Assume that this is a malformed request and
        // refuse to process it.
        info!("Chargeback: Transaction Client ID does not match record Client ID.");
        failed_transactions.push((
            record.clone(),
            TransactionError::ChargebackTransactionClientIDDoesNotMatchRequestClientID,
        ));
        return;
    }

    if transaction_ledger.get(&record.tx).unwrap().disputed_status == TransactionStatus::Undisputed
    {
        info!("Chargeback: Cannot Chargeback Undisputed Transaction.");
        failed_transactions.push((
            record.clone(),
            TransactionError::CannotChargebackUndisputedTransaction,
        ));
        return;
    }

    if transaction_ledger.get(&record.tx).unwrap().disputed_status
        == TransactionStatus::Chargebacked
    {
        info!("Chargeback: Cannot Chargeback Undisputed Transaction.");
        failed_transactions.push((
            record.clone(),
            TransactionError::CannotChargebackAlreadyChargebackedTransaction,
        ));
        return;
    }

    // Note that, while you cannot deposit or withdraw from a
    // locked acocunt, you most certainly can and should be
    // able to chargeback from such an account. In fact, if
    // someone is guilty of serial fraud, such a circumstance
    // is quite likely. As such, there's no check if the
    // account is already locked.
    let transaction_amount = get_transaction_amount(&record.tx, transaction_ledger);
    result.entry(record.client).and_modify(|client_data| {
        client_data.held -= transaction_amount;
        client_data.locked = true;
    });

    transaction_ledger
        .entry(record.tx)
        .and_modify(|entry| entry.disputed_status = TransactionStatus::Chargebacked);
}

fn get_transaction_amount(tx: &u32, transaction_ledger: &HashMap<u32, LedgerEntry>) -> Decimal {
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

mod tests {
    use super::*;

    #[test]
    fn test_dispute_nonexistant_transaction() {
        let mut result = get_default_ledger();
        let mut transaction_ledger: HashMap<u32, LedgerEntry> = HashMap::from([]);
        let mut failed_transactions: Vec<(LedgerEntry, TransactionError)> = vec![];

        let record = LedgerEntry {
            tx_type: TransactionType::Dispute,
            client: 1,
            tx: 1,
            amount: None,
            disputed_status: TransactionStatus::Undisputed,
        };

        process_transaction(
            &record,
            &mut result,
            &mut transaction_ledger,
            &mut failed_transactions,
        );

        assert_eq!(
            failed_transactions,
            vec![(
                record.clone(),
                TransactionError::CannotDisputeNonexistantTransaction
            )]
        );
        assert_eq!(transaction_ledger, HashMap::from([]));
        assert_eq!(result, get_default_ledger())
    }

    #[test]
    fn test_resolve_nonexistant_transaction() {
        let mut result = get_default_ledger();
        let mut transaction_ledger: HashMap<u32, LedgerEntry> = HashMap::from([]);
        let mut failed_transactions: Vec<(LedgerEntry, TransactionError)> = vec![];

        let record = LedgerEntry {
            tx_type: TransactionType::Resolve,
            client: 1,
            tx: 1,
            amount: None,
            disputed_status: TransactionStatus::Undisputed,
        };

        process_transaction(
            &record,
            &mut result,
            &mut transaction_ledger,
            &mut failed_transactions,
        );

        assert_eq!(
            failed_transactions,
            vec![(
                record.clone(),
                TransactionError::CannotResolveNonexistantTransaction
            )]
        );
        assert_eq!(transaction_ledger, HashMap::from([]));
        assert_eq!(result, get_default_ledger())
    }

    #[test]
    fn test_chargeback_nonexistant_transaction() {
        let mut result = get_default_ledger();
        let mut transaction_ledger: HashMap<u32, LedgerEntry> = HashMap::from([]);
        let mut failed_transactions: Vec<(LedgerEntry, TransactionError)> = vec![];

        let record = LedgerEntry {
            tx_type: TransactionType::Chargeback,
            client: 1,
            tx: 1,
            amount: None,
            disputed_status: TransactionStatus::Undisputed,
        };

        process_transaction(
            &record,
            &mut result,
            &mut transaction_ledger,
            &mut failed_transactions,
        );

        assert_eq!(
            failed_transactions,
            vec![(
                record.clone(),
                TransactionError::CannotChargebackNonexistantTransaction
            )]
        );
        assert_eq!(transaction_ledger, HashMap::from([]));
        assert_eq!(result, get_default_ledger())
    }

    #[test]
    fn test_dispute_nonexistant_customer() {
        let mut result = HashMap::from([]);
        let mut transaction_ledger: HashMap<u32, LedgerEntry> = HashMap::from([]);
        let mut failed_transactions: Vec<(LedgerEntry, TransactionError)> = vec![];

        let record = LedgerEntry {
            tx_type: TransactionType::Dispute,
            client: 1,
            tx: 1,
            amount: None,
            disputed_status: TransactionStatus::Undisputed,
        };

        process_transaction(
            &record,
            &mut result,
            &mut transaction_ledger,
            &mut failed_transactions,
        );

        assert_eq!(
            failed_transactions,
            vec![(
                record.clone(),
                TransactionError::CannotDisputeTransactionForNonexistantCustomer,
            )]
        );
        assert_eq!(transaction_ledger, HashMap::from([]));
        assert_eq!(result, HashMap::from([]))
    }

    #[test]
    fn test_resolve_nonexistant_customer() {
        let mut result = HashMap::from([]);
        let mut transaction_ledger: HashMap<u32, LedgerEntry> = HashMap::from([]);
        let mut failed_transactions: Vec<(LedgerEntry, TransactionError)> = vec![];

        let record = LedgerEntry {
            tx_type: TransactionType::Resolve,
            client: 1,
            tx: 1,
            amount: None,
            disputed_status: TransactionStatus::Undisputed,
        };

        process_transaction(
            &record,
            &mut result,
            &mut transaction_ledger,
            &mut failed_transactions,
        );

        assert_eq!(
            failed_transactions,
            vec![(
                record.clone(),
                TransactionError::CannotResolveTransactionForNonexistantCustomer,
            )]
        );
        assert_eq!(transaction_ledger, HashMap::from([]));
        assert_eq!(result, HashMap::from([]))
    }

    #[test]
    fn test_chargeback_nonexistant_customer() {
        let mut result = HashMap::from([]);
        let mut transaction_ledger: HashMap<u32, LedgerEntry> = HashMap::from([]);
        let mut failed_transactions: Vec<(LedgerEntry, TransactionError)> = vec![];

        let record = LedgerEntry {
            tx_type: TransactionType::Chargeback,
            client: 1,
            tx: 1,
            amount: None,
            disputed_status: TransactionStatus::Undisputed,
        };

        process_transaction(
            &record,
            &mut result,
            &mut transaction_ledger,
            &mut failed_transactions,
        );

        assert_eq!(
            failed_transactions,
            vec![(
                record.clone(),
                TransactionError::CannotChargebackTransactionForNonexistantCustomer,
            )]
        );
        assert_eq!(transaction_ledger, HashMap::from([]));
        assert_eq!(result, HashMap::from([]))
    }

    fn get_default_ledger() -> HashMap<u16, AccountInfo> {
        HashMap::from([
            (
                1,
                AccountInfo {
                    available: dec!(50.0),
                    held: dec!(0.0),
                    locked: false,
                },
            ),
            (
                2,
                AccountInfo {
                    available: dec!(0.0),
                    held: dec!(0.0),
                    locked: true,
                },
            ),
            (
                3,
                AccountInfo {
                    available: dec!(500.0),
                    held: dec!(100.0),
                    locked: false,
                },
            ),
        ])
    }

    #[test]
    fn test_deposit_on_empty_ledger() {
        let mut result = HashMap::from([]);
        let mut transaction_ledger: HashMap<u32, LedgerEntry> = HashMap::from([]);
        let mut failed_transactions: Vec<(LedgerEntry, TransactionError)> = vec![];

        let record = LedgerEntry {
            tx_type: TransactionType::Deposit,
            client: 1,
            tx: 1,
            amount: Some(dec!(50.0)),
            disputed_status: TransactionStatus::Undisputed,
        };

        process_transaction(
            &record,
            &mut result,
            &mut transaction_ledger,
            &mut failed_transactions,
        );

        assert_eq!(failed_transactions, vec![]);
        assert_eq!(transaction_ledger, HashMap::from([(1, record.clone())]));
        assert_eq!(
            result,
            HashMap::from([(
                1,
                AccountInfo {
                    available: dec!(50.0),
                    held: dec!(0.0),
                    locked: false
                }
            )])
        );
    }

    #[test]
    fn test_deposit_missing_amount_field() {
        let mut result = HashMap::from([]);
        let mut transaction_ledger: HashMap<u32, LedgerEntry> = HashMap::from([]);
        let mut failed_transactions: Vec<(LedgerEntry, TransactionError)> = vec![];

        let record = LedgerEntry {
            tx_type: TransactionType::Deposit,
            client: 1,
            tx: 1,
            amount: None,
            disputed_status: TransactionStatus::Undisputed,
        };

        process_transaction(
            &record,
            &mut result,
            &mut transaction_ledger,
            &mut failed_transactions,
        );

        assert_eq!(
            failed_transactions,
            vec![(record.clone(), TransactionError::MissingAmountField)]
        );
        assert_eq!(transaction_ledger, HashMap::from([]));
        assert_eq!(result, HashMap::from([]));
    }

    #[test]
    fn test_withdraw_missing_amount_field() {
        let mut result = HashMap::from([]);
        let mut transaction_ledger: HashMap<u32, LedgerEntry> = HashMap::from([]);
        let mut failed_transactions: Vec<(LedgerEntry, TransactionError)> = vec![];

        let record = LedgerEntry {
            tx_type: TransactionType::Withdrawal,
            client: 1,
            tx: 1,
            amount: None,
            disputed_status: TransactionStatus::Undisputed,
        };

        process_transaction(
            &record,
            &mut result,
            &mut transaction_ledger,
            &mut failed_transactions,
        );

        assert_eq!(
            failed_transactions,
            vec![(record.clone(), TransactionError::MissingAmountField)]
        );
        assert_eq!(transaction_ledger, HashMap::from([]));
        assert_eq!(result, HashMap::from([]));
    }
    #[test]
    fn test_deposit_on_populated_ledger() {
        let mut result = get_default_ledger();
        let mut expected_result = get_default_ledger();
        expected_result
            .entry(1)
            .and_modify(|client_info| client_info.available += dec!(50.0));
        let mut transaction_ledger: HashMap<u32, LedgerEntry> = HashMap::from([]);
        let mut failed_transactions: Vec<(LedgerEntry, TransactionError)> = vec![];

        let record = LedgerEntry {
            tx_type: TransactionType::Deposit,
            client: 1,
            tx: 1,
            amount: Some(dec!(50.0)),
            disputed_status: TransactionStatus::Undisputed,
        };

        process_transaction(
            &record,
            &mut result,
            &mut transaction_ledger,
            &mut failed_transactions,
        );

        assert_eq!(failed_transactions, vec![]);
        assert_eq!(result, expected_result);
        assert_eq!(transaction_ledger, HashMap::from([(1, record.clone())]));
    }

    #[test]
    fn test_deposit_on_locked_account() {
        let mut result = get_default_ledger();
        let expected_result = get_default_ledger();
        let mut transaction_ledger: HashMap<u32, LedgerEntry> = HashMap::from([]);
        let mut failed_transactions: Vec<(LedgerEntry, TransactionError)> = vec![];

        let record = LedgerEntry {
            tx_type: TransactionType::Deposit,
            client: 2,
            tx: 1,
            amount: Some(dec!(50.0)),
            disputed_status: TransactionStatus::Undisputed,
        };

        let expected_failed_transactions = vec![(
            record.clone(),
            TransactionError::CannotDepoistIntoLockedAccount,
        )];

        process_transaction(
            &record,
            &mut result,
            &mut transaction_ledger,
            &mut failed_transactions,
        );

        assert_eq!(failed_transactions, expected_failed_transactions);
        assert_eq!(result, expected_result);
        assert_eq!(transaction_ledger, HashMap::from([]));
    }

    #[test]
    fn test_deposit_duplicate_transaction_id() {
        let mut result = get_default_ledger();
        let expected_result = get_default_ledger();

        let record = LedgerEntry {
            tx_type: TransactionType::Deposit,
            client: 1,
            tx: 1,
            amount: Some(dec!(50.0)),
            disputed_status: TransactionStatus::Undisputed,
        };

        let mut transaction_ledger: HashMap<u32, LedgerEntry> =
            HashMap::from([(1, record.clone())]);
        let expected_transaction_ledger: HashMap<u32, LedgerEntry> = transaction_ledger.clone();
        let mut failed_transactions: Vec<(LedgerEntry, TransactionError)> = vec![];
        let expected_failed_transactions =
            vec![(record.clone(), TransactionError::DuplicateTransactionId)];

        process_transaction(
            &record,
            &mut result,
            &mut transaction_ledger,
            &mut failed_transactions,
        );

        assert_eq!(failed_transactions, expected_failed_transactions);
        assert_eq!(result, expected_result);
        assert_eq!(transaction_ledger, expected_transaction_ledger);
    }

    #[test]
    fn test_withdraw_duplicate_transaction_id() {
        let mut result = get_default_ledger();
        let expected_result = get_default_ledger();

        let record = LedgerEntry {
            tx_type: TransactionType::Withdrawal,
            client: 1,
            tx: 1,
            amount: Some(dec!(50.0)),
            disputed_status: TransactionStatus::Undisputed,
        };

        let mut transaction_ledger: HashMap<u32, LedgerEntry> =
            HashMap::from([(1, record.clone())]);
        let expected_transaction_ledger: HashMap<u32, LedgerEntry> = transaction_ledger.clone();
        let mut failed_transactions: Vec<(LedgerEntry, TransactionError)> = vec![];
        let expected_failed_transactions =
            vec![(record.clone(), TransactionError::DuplicateTransactionId)];

        process_transaction(
            &record,
            &mut result,
            &mut transaction_ledger,
            &mut failed_transactions,
        );

        assert_eq!(failed_transactions, expected_failed_transactions);
        assert_eq!(result, expected_result);
        assert_eq!(transaction_ledger, expected_transaction_ledger);
    }

    #[test]
    fn test_withdraw_overdraft() {
        let mut result = get_default_ledger();
        let expected_result = get_default_ledger();

        let record = LedgerEntry {
            tx_type: TransactionType::Withdrawal,
            client: 1,
            tx: 1,
            amount: Some(dec!(50_000.0)),
            disputed_status: TransactionStatus::Undisputed,
        };

        let mut transaction_ledger: HashMap<u32, LedgerEntry> = HashMap::from([]);
        let expected_transaction_ledger: HashMap<u32, LedgerEntry> = transaction_ledger.clone();
        let mut failed_transactions: Vec<(LedgerEntry, TransactionError)> = vec![];
        let expected_failed_transactions =
            vec![(record.clone(), TransactionError::CannotOverdrawByWithdrawal)];

        process_transaction(
            &record,
            &mut result,
            &mut transaction_ledger,
            &mut failed_transactions,
        );

        assert_eq!(failed_transactions, expected_failed_transactions);
        assert_eq!(result, expected_result);
        assert_eq!(transaction_ledger, expected_transaction_ledger);
    }

    #[test]
    fn test_withdraw_from_locked_account() {
        let mut result = get_default_ledger();
        let expected_result = get_default_ledger();
        let mut transaction_ledger: HashMap<u32, LedgerEntry> = HashMap::from([]);
        let mut failed_transactions: Vec<(LedgerEntry, TransactionError)> = vec![];

        let record = LedgerEntry {
            tx_type: TransactionType::Withdrawal,
            client: 2,
            tx: 1,
            amount: Some(dec!(50.0)),
            disputed_status: TransactionStatus::Undisputed,
        };

        let expected_failed_transactions = vec![(
            record.clone(),
            TransactionError::CannotWithdrawFromLockedAccount,
        )];

        process_transaction(
            &record,
            &mut result,
            &mut transaction_ledger,
            &mut failed_transactions,
        );

        assert_eq!(failed_transactions, expected_failed_transactions);
        assert_eq!(result, expected_result);
        assert_eq!(transaction_ledger, HashMap::from([]));
    }

    #[test]
    fn test_withdraw_from_nonexistant_client() {
        let mut result = get_default_ledger();
        let expected_result = get_default_ledger();
        let mut transaction_ledger: HashMap<u32, LedgerEntry> = HashMap::from([]);
        let mut failed_transactions: Vec<(LedgerEntry, TransactionError)> = vec![];

        let record = LedgerEntry {
            tx_type: TransactionType::Withdrawal,
            client: 5,
            tx: 1,
            amount: Some(dec!(50.0)),
            disputed_status: TransactionStatus::Undisputed,
        };

        let expected_failed_transactions = vec![(
            record.clone(),
            TransactionError::CannotWithdrawFromNonexistantCustomer,
        )];

        process_transaction(
            &record,
            &mut result,
            &mut transaction_ledger,
            &mut failed_transactions,
        );

        assert_eq!(failed_transactions, expected_failed_transactions);
        assert_eq!(result, expected_result);
        assert_eq!(transaction_ledger, HashMap::from([]));
    }

    #[test]
    fn test_dispute_on_nonexistant_client() {
        let mut result = get_default_ledger();
        let expected_result = get_default_ledger();
        let mut transaction_ledger: HashMap<u32, LedgerEntry> = HashMap::from([]);
        let mut failed_transactions: Vec<(LedgerEntry, TransactionError)> = vec![];

        let record = LedgerEntry {
            tx_type: TransactionType::Dispute,
            client: 5,
            tx: 1,
            amount: None,
            disputed_status: TransactionStatus::Undisputed,
        };

        let expected_failed_transactions = vec![(
            record.clone(),
            TransactionError::CannotDisputeTransactionForNonexistantCustomer,
        )];

        process_transaction(
            &record,
            &mut result,
            &mut transaction_ledger,
            &mut failed_transactions,
        );

        assert_eq!(failed_transactions, expected_failed_transactions);
        assert_eq!(result, expected_result);
        assert_eq!(transaction_ledger, HashMap::from([]));
    }

    fn get_default_transactions() -> HashMap<u32, LedgerEntry> {
        HashMap::from([
            (
                1,
                LedgerEntry {
                    tx_type: TransactionType::Withdrawal,
                    client: 1,
                    tx: 1,
                    amount: Some(dec!(50.0)),
                    disputed_status: TransactionStatus::Undisputed,
                },
            ),
            (
                2,
                LedgerEntry {
                    tx_type: TransactionType::Deposit,
                    client: 1,
                    tx: 2,
                    amount: None,
                    disputed_status: TransactionStatus::Disputed,
                },
            ),
            (
                3,
                LedgerEntry {
                    tx_type: TransactionType::Dispute,
                    client: 1,
                    tx: 2,
                    amount: None,
                    disputed_status: TransactionStatus::Undisputed,
                },
            ),
            (
                4,
                LedgerEntry {
                    tx_type: TransactionType::Dispute,
                    client: 3,
                    tx: 4,
                    amount: Some(dec!(50.0)),
                    disputed_status: TransactionStatus::Disputed,
                },
            ),
        ])
    }

    #[test]
    fn test_dispute_already_disputed_transaction() {
        let mut result = get_default_ledger();
        let expected_result = get_default_ledger();

        let record = LedgerEntry {
            tx_type: TransactionType::Dispute,
            client: 1,
            tx: 2,
            amount: None,
            disputed_status: TransactionStatus::Undisputed,
        };

        let mut transaction_ledger: HashMap<u32, LedgerEntry> = get_default_transactions();
        let expected_transaction_ledger: HashMap<u32, LedgerEntry> = get_default_transactions();
        let mut failed_transactions: Vec<(LedgerEntry, TransactionError)> = vec![];
        let expected_failed_transactions = vec![(
            record.clone(),
            TransactionError::CannotDisputeAlreadyDisputedTransaction,
        )];

        process_transaction(
            &record,
            &mut result,
            &mut transaction_ledger,
            &mut failed_transactions,
        );

        assert_eq!(failed_transactions, expected_failed_transactions);
        assert_eq!(result, expected_result);
        assert_eq!(transaction_ledger, expected_transaction_ledger);
    }

    #[test]
    fn test_resolve_undisputed_transaction() {
        let mut result = get_default_ledger();
        let expected_result = get_default_ledger();

        let record = LedgerEntry {
            tx_type: TransactionType::Resolve,
            client: 1,
            tx: 1,
            amount: None,
            disputed_status: TransactionStatus::Undisputed,
        };

        let mut transaction_ledger: HashMap<u32, LedgerEntry> = get_default_transactions();
        let expected_transaction_ledger: HashMap<u32, LedgerEntry> = get_default_transactions();
        let mut failed_transactions: Vec<(LedgerEntry, TransactionError)> = vec![];
        let expected_failed_transactions = vec![(
            record.clone(),
            TransactionError::CannotResolveUndisputedTransaction,
        )];

        process_transaction(
            &record,
            &mut result,
            &mut transaction_ledger,
            &mut failed_transactions,
        );

        assert_eq!(failed_transactions, expected_failed_transactions);
        assert_eq!(result, expected_result);
        assert_eq!(transaction_ledger, expected_transaction_ledger);
    }

    #[test]
    fn test_chargeback_undisputed_transaction() {
        let mut result = get_default_ledger();
        let expected_result = get_default_ledger();

        let record = LedgerEntry {
            tx_type: TransactionType::Chargeback,
            client: 1,
            tx: 1,
            amount: None,
            disputed_status: TransactionStatus::Undisputed,
        };

        let mut transaction_ledger: HashMap<u32, LedgerEntry> = get_default_transactions();
        let expected_transaction_ledger: HashMap<u32, LedgerEntry> = get_default_transactions();
        let mut failed_transactions: Vec<(LedgerEntry, TransactionError)> = vec![];
        let expected_failed_transactions = vec![(
            record.clone(),
            TransactionError::CannotChargebackUndisputedTransaction,
        )];

        process_transaction(
            &record,
            &mut result,
            &mut transaction_ledger,
            &mut failed_transactions,
        );

        assert_eq!(failed_transactions, expected_failed_transactions);
        assert_eq!(result, expected_result);
        assert_eq!(transaction_ledger, expected_transaction_ledger);
    }

    #[test]
    fn test_dispute_mismatched_client_id() {
        let mut result = get_default_ledger();
        let expected_result = get_default_ledger();

        let record = LedgerEntry {
            tx_type: TransactionType::Dispute,
            client: 2,
            tx: 1,
            amount: None,
            disputed_status: TransactionStatus::Undisputed,
        };

        let mut transaction_ledger: HashMap<u32, LedgerEntry> = get_default_transactions();
        let expected_transaction_ledger: HashMap<u32, LedgerEntry> = get_default_transactions();
        let mut failed_transactions: Vec<(LedgerEntry, TransactionError)> = vec![];
        let expected_failed_transactions = vec![(
            record.clone(),
            TransactionError::DisputeTransactionClientIDDoesNotMatchRequestClientID,
        )];

        process_transaction(
            &record,
            &mut result,
            &mut transaction_ledger,
            &mut failed_transactions,
        );

        assert_eq!(failed_transactions, expected_failed_transactions);
        assert_eq!(result, expected_result);
        assert_eq!(transaction_ledger, expected_transaction_ledger);
    }

    #[test]
    fn test_resolve_mismatched_client_id() {
        let mut result = get_default_ledger();
        let expected_result = get_default_ledger();

        let record = LedgerEntry {
            tx_type: TransactionType::Resolve,
            client: 2,
            tx: 1,
            amount: None,
            disputed_status: TransactionStatus::Undisputed,
        };

        let mut transaction_ledger: HashMap<u32, LedgerEntry> = get_default_transactions();
        let expected_transaction_ledger: HashMap<u32, LedgerEntry> = get_default_transactions();
        let mut failed_transactions: Vec<(LedgerEntry, TransactionError)> = vec![];
        let expected_failed_transactions = vec![(
            record.clone(),
            TransactionError::ResolveTransactionClientIDDoesNotMatchRequestClientID,
        )];

        process_transaction(
            &record,
            &mut result,
            &mut transaction_ledger,
            &mut failed_transactions,
        );

        assert_eq!(failed_transactions, expected_failed_transactions);
        assert_eq!(result, expected_result);
        assert_eq!(transaction_ledger, expected_transaction_ledger);
    }

    #[test]
    fn test_chargeback_mismatched_client_id() {
        let mut result = get_default_ledger();
        let expected_result = get_default_ledger();

        let record = LedgerEntry {
            tx_type: TransactionType::Chargeback,
            client: 2,
            tx: 1,
            amount: None,
            disputed_status: TransactionStatus::Undisputed,
        };

        let mut transaction_ledger: HashMap<u32, LedgerEntry> = get_default_transactions();
        let expected_transaction_ledger: HashMap<u32, LedgerEntry> = get_default_transactions();
        let mut failed_transactions: Vec<(LedgerEntry, TransactionError)> = vec![];
        let expected_failed_transactions = vec![(
            record.clone(),
            TransactionError::ChargebackTransactionClientIDDoesNotMatchRequestClientID,
        )];

        process_transaction(
            &record,
            &mut result,
            &mut transaction_ledger,
            &mut failed_transactions,
        );

        assert_eq!(failed_transactions, expected_failed_transactions);
        assert_eq!(result, expected_result);
        assert_eq!(transaction_ledger, expected_transaction_ledger);
    }

    #[test]
    fn test_successful_dispute() {
        let mut result = get_default_ledger();
        let mut expected_result = get_default_ledger();
        expected_result.entry(1).and_modify(|client_info| {
            client_info.available = dec!(0.0);
            client_info.held = dec!(50.0);
        });

        let record = LedgerEntry {
            tx_type: TransactionType::Dispute,
            client: 1,
            tx: 1,
            amount: None,
            disputed_status: TransactionStatus::Undisputed,
        };

        let mut transaction_ledger: HashMap<u32, LedgerEntry> = get_default_transactions();
        let mut expected_transaction_ledger: HashMap<u32, LedgerEntry> = get_default_transactions();
        expected_transaction_ledger
            .entry(1)
            .and_modify(|entry| entry.disputed_status = TransactionStatus::Disputed);
        let mut failed_transactions: Vec<(LedgerEntry, TransactionError)> = vec![];
        let expected_failed_transactions = vec![];

        process_transaction(
            &record,
            &mut result,
            &mut transaction_ledger,
            &mut failed_transactions,
        );

        assert_eq!(failed_transactions, expected_failed_transactions);
        assert_eq!(result, expected_result);
        assert_eq!(transaction_ledger, expected_transaction_ledger);
    }

    #[test]
    fn test_successful_resolution() {
        let mut result = get_default_ledger();
        let mut expected_result = get_default_ledger();
        expected_result.entry(3).and_modify(|client_info| {
            client_info.available += dec!(50.0);
            client_info.held -= dec!(50.0);
        });

        let record = LedgerEntry {
            tx_type: TransactionType::Resolve,
            client: 3,
            tx: 4,
            amount: None,
            disputed_status: TransactionStatus::Undisputed,
        };

        let mut transaction_ledger: HashMap<u32, LedgerEntry> = get_default_transactions();
        let mut expected_transaction_ledger: HashMap<u32, LedgerEntry> = get_default_transactions();
        expected_transaction_ledger
            .entry(4)
            .and_modify(|entry| entry.disputed_status = TransactionStatus::Undisputed);
        let mut failed_transactions: Vec<(LedgerEntry, TransactionError)> = vec![];
        let expected_failed_transactions = vec![];

        process_transaction(
            &record,
            &mut result,
            &mut transaction_ledger,
            &mut failed_transactions,
        );

        assert_eq!(failed_transactions, expected_failed_transactions);
        assert_eq!(result, expected_result);
        assert_eq!(transaction_ledger, expected_transaction_ledger);
    }

    #[test]
    fn test_successful_chargeback() {
        let mut result = get_default_ledger();
        let mut expected_result = get_default_ledger();
        expected_result.entry(3).and_modify(|client_info| {
            client_info.held -= dec!(50.0);
            client_info.locked = true;
        });

        let record = LedgerEntry {
            tx_type: TransactionType::Chargeback,
            client: 3,
            tx: 4,
            amount: None,
            disputed_status: TransactionStatus::Undisputed,
        };

        let mut transaction_ledger: HashMap<u32, LedgerEntry> = get_default_transactions();
        let mut expected_transaction_ledger: HashMap<u32, LedgerEntry> = get_default_transactions();
        expected_transaction_ledger.entry(4).and_modify(|entry| {
            entry.disputed_status = TransactionStatus::Chargebacked;
        });
        let mut failed_transactions: Vec<(LedgerEntry, TransactionError)> = vec![];
        let expected_failed_transactions = vec![];

        process_transaction(
            &record,
            &mut result,
            &mut transaction_ledger,
            &mut failed_transactions,
        );

        assert_eq!(failed_transactions, expected_failed_transactions);
        assert_eq!(result, expected_result);
        assert_eq!(transaction_ledger, expected_transaction_ledger);
    }

    #[tokio::test]
    async fn test_whitespace_deposits() -> Result<()> {
        let ledger = process_transactions("src/deposit.csv").await?;
        assert_eq!(
            ledger,
            HashMap::from([
                (
                    1,
                    AccountInfo {
                        available: dec!(500.0),
                        held: dec!(0.0),
                        locked: false,
                    },
                ),
                (
                    2,
                    AccountInfo {
                        available: dec!(600.0),
                        held: dec!(0.0),
                        locked: false,
                    },
                ),
                (
                    3,
                    AccountInfo {
                        available: dec!(1000.0),
                        held: dec!(0.0),
                        locked: false,
                    },
                ),
                (
                    4,
                    AccountInfo {
                        available: dec!(10_000.0),
                        held: dec!(0.0),
                        locked: false,
                    },
                ),
            ]),
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_million_rows() -> Result<()> {
        let ledger = process_transactions("src/1m_rows.csv").await?;
        assert_eq!(
            ledger,
            HashMap::from([(
                1,
                AccountInfo {
                    available: dec!(50_000_000.00),
                    held: dec!(0.0),
                    locked: false,
                }
            )])
        );

        Ok(())
    }

    // Why have this test if we already have a 1m row test? Making
    // sure we can get up into 50b in available without losing
    // precision is very valuable. As floating-point gets larger, it
    // loses precision, so we're making sure we're getting
    // value-for-money (as spent as cognitive overhead and build time)
    // for the types we're using.
    #[tokio::test]
    async fn test_million_rows_large_transactions() -> Result<()> {
        let ledger = process_transactions("src/1m_rows_large_transactions.csv").await?;
        assert_eq!(
            ledger,
            HashMap::from([(
                1,
                AccountInfo {
                    available: dec!(50_000_000_000.00),
                    held: dec!(0.0),
                    locked: false,
                }
            )])
        );

        Ok(())
    }

    // CannotChargebackAlreadyChargebackedTransaction
}
