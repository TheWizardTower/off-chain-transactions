use anyhow::Result;
use log::{debug, info};
use rust_decimal::prelude::*;
use std::collections::HashMap;
use std::path::Path;
use tokio::fs::File;
use tokio_stream::StreamExt;

use crate::types::*;

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
pub async fn process_transactions(filename: impl AsRef<Path>) -> Result<HashMap<u16, AccountInfo>> {
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
        if record.is_err() {
            // If we encounter a corrupted row, don't process it, but
            // don't abort the overall processing job.
            continue;
        }
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
//
// This function is marked public simply for testing purposes. It may
// be useful for customers, but the intended entry point is
// process_transactions, above.
pub fn process_transaction(
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
// Assuming all these conditionals are satisfied, we roll back the
// selected transaction. This means that if it was a deposit, we debit
// the amount from Available into Held, and if it was a Withdrawal, we
// credit the amount into Held. Then, we mark the transaction as
// disputed in the transaction ledger.
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
    match transaction_ledger.get(&record.tx).unwrap().tx_type {
        TransactionType::Deposit => {
            result.entry(record.client).and_modify(|client_data| {
                client_data.available -= transaction_amount;
                client_data.held += transaction_amount;
            });
        }
        TransactionType::Withdrawal => {
            result.entry(record.client).and_modify(|client_data| {
                client_data.held += transaction_amount;
            });
        }
        // TODO: Find a nicer thing to do, here.
        _ => return,
    }
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
    match transaction_ledger.get(&record.tx).unwrap().tx_type {
        TransactionType::Deposit => {
            result.entry(record.client).and_modify(|client_data| {
                client_data.available += transaction_amount;
                client_data.held -= transaction_amount;
            });
        }
        TransactionType::Withdrawal => {
            result.entry(record.client).and_modify(|client_data| {
                client_data.held -= transaction_amount;
            });
        }
        // TODO: Find a nicer thing to do, here.
        _ => return,
    }
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

    match transaction_ledger.get(&record.tx).unwrap().tx_type {
        TransactionType::Deposit => {
            result.entry(record.client).and_modify(|client_data| {
                client_data.held -= transaction_amount;
                client_data.locked = true;
            });
        }
        TransactionType::Withdrawal => {
            result.entry(record.client).and_modify(|client_data| {
                client_data.held -= transaction_amount;
                client_data.available += transaction_amount;
            });
        }
        // TODO: Find a nicer thing to do, here.
        _ => return,
    }

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
