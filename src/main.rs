use anyhow::Result;
use log::{debug, error, info};
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

// This is a helper function for the disputed_status field in the
// LedgerEntry struct above. This allows us to set a default value for
// a field that Serde is not expected to serialize up from the CSV
// rows we're taking in.
fn disputed_default() -> TransactionStatus {
    TransactionStatus::Undisputed
}

// A list of all the failures we know about in our program. We don't
// expose these to the user as yet, but these have proven enormously
// useful in testing, to prove that we got to the code path we
// expected to. It's also paving the way later on for giving feedback
// to the user, which is always a good thing.
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
    CannotResolveForLockedAccount,
}

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

// This is a utility function to render out the ledger to stdout.
// Since we take in a map of u16 -> AccountInfo, we have to massage
// the data a bit before sending it out.
//
// First, since we haven't tracked it while processing transactions,
// we calculate the Total field for an account, which should always be
// available + held.
//
// Second, since we kept the client ID as the key to the hash (in
// deference to the assumed invariant of every client having only one
// account), we put that field back in.
//
// Once the munging is done, we render it out (with headers) as CSV to
// stdout.
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

// This is a utility function for write_ledger, its purpose is to
// convert betwen an AccountInfo struct and an AccountInfoWithTotal
// struct. This latter type has two differences:
//
// 1. There's a new total field, which is equal to avaialble + held pools.
// 2. The client ID is recorded in the struct, rather than being the
// key in the hashmap.
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

// Utility predicate. This is important to check with every
// transaction type we run. If it's a deposit or withdrawal, it should
// have a unique (i.e., new) transaction ID. Disputes, Resolutions,
// and Chargebacks, however, refer to *already-existing* transaction
// IDs. Either way, having this around is very useful.
fn transaction_id_is_new(txid: &u32, transaction_ledger: &HashMap<u32, LedgerEntry>) -> bool {
    !transaction_ledger.contains_key(txid)
}

// This function sets up the IO that our business logic will work in.
// We take in a filename, open that file, wire it up to an async
// deserializer from the csv_async crate, and then call
// process_transaction on the resulting record.
//
// Note that we also create a number of mutable data structures.
//
// `result` is the ledger of client information we're expected to
// write out to stdout as CSV at the end of execution,
//
// `transaction_ledger` is the set of successful deposits and
// withdrawals, which enables us to refer back in the case of
// Disputes, Resolutions, and Chargebacks.
//
// `failed_transactions` is a record of what transactions were
// rejected, and why. This point in particular is tremendously useful
// for testing.
async fn process_transactions(filename: impl AsRef<Path>) -> Result<HashMap<u16, AccountInfo>> {
    let mut result: HashMap<u16, AccountInfo> = HashMap::from([]);
    let mut transaction_ledger: HashMap<u32, LedgerEntry> = HashMap::from([]);
    let mut failed_transactions: Vec<(LedgerEntry, TransactionError)> = vec![];
    let file_handle = File::open(filename).await?;
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

// process_transaction is the start of the core business logic. All
// this function does is dispatch from the transaction type to the
// appropriate process_<type> function. However, from here on out, the
// only IO you're going to see should be either 1) logging, or 2)
// mutating data structures we've been given mutable references to.
// This makes testing much easier to set up and do.
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

// Process Deposits. These are probably the most lax type of
// transactions. We don't even require that a customer exist, we
// presume that this is the initial deposit on account creation. We
// check if the transaction ID is new, if the amount field is present,
// and if the account both exists and is locked from an earlier
// chargeback. If the transaction fails, we add it to
// failed_transactions as a tuple, along with the reason for
// rejection, as represented by the TransactionError type.
// If all is well, we either add to an existing account, or
// create a new one with the amount balance. Lastly, we add the
// deposit record to the transaction_ledger in case of future dispute.
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

// Process withdrawals. We check the same things as with Deposits,
// with the added constraint that we cannot withdraw money from a
// non-existant account, and we cannot overdraft an existing account.
// If something is found to be wrong, we add it to failed_transactions
// as a tuple, along with the reason the withdrawal was rejected as
// represented by the TransactionError type. If all sanity checks
// pass, we remove the amount from the acocunt, and add the
// transaciton to transaction_ledger for later reference in case of
// dispute.
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

// process_dispute has a bit more homework to do. There are a number of conditions that have to be met, which we will go through here:
//
// 1. The customer ID in the record we were given must refer to an actual, pre-existing customer.
// 2. The transaction ID in the record we were given must refer to an actual, pre-existing transaction.
// 3. The transaction's customer ID and the record's customer ID must match.
// 4. The transaction in question must not already be either in dispute or chargebacked.
//
// Assuming all these conditionals are satisfied, we move the
// transaction amount from available to held in the customer's
// account, and mark the transaction as disputed in the transaction
// ledger.
//
// Note that we cannot record this LedgerEntry value in the
// transaction_ledger, as we were not given a unique transaction ID
// for it. We could, in the future, add a field that Serde didn't
// expect to serialize in or out, that recorded the history of
// disputes, resolutions, and (if any) ultimate chargeback, if we so
// chose. However, this does give us an advantage in that we cannot
// get caught up in "meta-disputes", i.e., disputes _about_ disputes,
// resolutions, or chargebacks.
fn process_dispute(
    record: &LedgerEntry,
    result: &mut HashMap<u16, AccountInfo>,
    transaction_ledger: &mut HashMap<u32, LedgerEntry>,
    failed_transactions: &mut Vec<(LedgerEntry, TransactionError)>,
) {
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

// similar to process_dispute, we have a number of conditionals to check before processing this.
//
// 1. The customer ID in the record we were given must refer to an actual, pre-existing customer.
// 2. The transaction ID in the record we were given must refer to an actual, pre-existing transaction.
// 3. The transaction's customer ID and the record's customer ID must match.
// 4. The client in question must have a valid (i.e., not locked) account.
// 5. The transaction in question must be disputed, it cannot be undisputed or already chargebacked.
//
// Assuming all is satisfied, we then move the transaction amount from
// held to available, and mark the transaction as undisputed in the
// transaction ledger.
//
// Not also that, like disputes and chargebacks, we cannot record this
// LedgerEntry value in the transaction ledger, as we have no unique
// transaction ID for it. However, this prevents us from getting
// caught up in "meta-disputes", or disputes about disputes,
// resolutions, or chargebacks.
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

    if result.get(&record.client).unwrap().locked {
        // If your account is locked, you probably ought to speak with
        // a human, rather than the automated system. Refuse to
        // proceed further until that is sorted out.
        info!("Resolve: Cannot resolve a transaction for a locked account.");
        failed_transactions.push((
            record.clone(),
            TransactionError::CannotResolveForLockedAccount,
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

    let transaction_amount = get_transaction_amount(&record.tx, transaction_ledger);
    result.entry(record.client).and_modify(|client_data| {
        client_data.available += transaction_amount;
        client_data.held -= transaction_amount;
    });
    transaction_ledger
        .entry(record.tx)
        .and_modify(|entry| entry.disputed_status = TransactionStatus::Undisputed);
}

// process_chargeback, like process_dispute and process_resolve, a number of predicates to check.
//
// 1. The customer ID in the record we were given must refer to an actual, pre-existing customer.
// 2. The transaction ID in the record we were given must refer to an actual, pre-existing transaction.
// 3. The transaction's customer ID and the record's customer ID must match.
// 4. The transaction in question must be in dispute, it cannot be either undisputet or already chargebacked.
//
// If something is amiss, we add the record to failed_transaction,
// along with a TransactionError type explaining why.
//
// If all is well, we withdraw the transaction amount from held, mark
// that client's account as locked, and mark the transaction as
// chargebacked.
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

// This is a utility function for dispute, resolve, and chargeback
// functions. They don't carry any information about the amount of the
// transaction, relying on us to record that information. We access
// that here. Note that we directly unwrap, the assumption is that the
// calling function has already validated that the transaction ID is
// valid.
fn get_transaction_amount(tx: &u32, transaction_ledger: &HashMap<u32, LedgerEntry>) -> Decimal {
    transaction_ledger.get(tx).unwrap().amount.unwrap()
}

// A note about the tests. First, every TransactionError should be
// exercised here. Second, we should also exercise the
// happy/successful path, and make sure that each transaction updates
// the data structures as it should, and leaves the others alone.
// Third, we also have two tests at the end that are more of an
// integration/performance test. That is, we call the
// process_transactions function on a file with 1 million rows in the
// repo, and ensure that we get a correct result. This validates that
// our math doesn't encur error, even in large values, or repeated
// small operations. It may be desirable to make those hidden behind a
// feature flag, so they don't slow down the much faster unit tests of
// process_transaction.
//
// There are two utility functions here, get_default_ledger and
// get_default_transactions, which are used in a number of tests,
// here. The values here are chosen with care, to ensure we hit the
// code paths we expect.
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
                    tx_type: TransactionType::Deposit,
                    client: 1,
                    tx: 2,
                    amount: Some(dec!(50.0)),
                    disputed_status: TransactionStatus::Undisputed,
                },
            ),
            (
                4,
                LedgerEntry {
                    tx_type: TransactionType::Deposit,
                    client: 3,
                    tx: 4,
                    amount: Some(dec!(50.0)),
                    disputed_status: TransactionStatus::Disputed,
                },
            ),
            (
                5,
                LedgerEntry {
                    tx_type: TransactionType::Withdrawal,
                    client: 2,
                    tx: 4,
                    amount: Some(dec!(50.0)),
                    disputed_status: TransactionStatus::Chargebacked,
                },
            ),
            (
                6,
                LedgerEntry {
                    tx_type: TransactionType::Deposit,
                    client: 2,
                    tx: 6,
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
    fn test_resolve_transaction_for_locked_account() {
        let mut result = get_default_ledger();
        let expected_result = get_default_ledger();

        let record = LedgerEntry {
            tx_type: TransactionType::Resolve,
            client: 2,
            tx: 6,
            amount: None,
            disputed_status: TransactionStatus::Undisputed,
        };

        let mut transaction_ledger: HashMap<u32, LedgerEntry> = get_default_transactions();
        let expected_transaction_ledger: HashMap<u32, LedgerEntry> = get_default_transactions();
        let mut failed_transactions: Vec<(LedgerEntry, TransactionError)> = vec![];
        let expected_failed_transactions = vec![(
            record.clone(),
            TransactionError::CannotResolveForLockedAccount,
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

    #[test]
    fn test_chargeback_on_chargebacked_transaction() {
        let mut result = get_default_ledger();
        let expected_result = get_default_ledger();

        let record = LedgerEntry {
            tx_type: TransactionType::Chargeback,
            client: 2,
            tx: 5,
            amount: None,
            disputed_status: TransactionStatus::Undisputed,
        };

        let mut transaction_ledger: HashMap<u32, LedgerEntry> = get_default_transactions();
        let expected_transaction_ledger: HashMap<u32, LedgerEntry> = get_default_transactions();
        let mut failed_transactions: Vec<(LedgerEntry, TransactionError)> = vec![];
        let expected_failed_transactions = vec![(
            record.clone(),
            TransactionError::CannotChargebackAlreadyChargebackedTransaction,
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
}
