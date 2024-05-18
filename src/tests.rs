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

use super::*;
// We don't import this in the main module, so we have to pull it in here.
use crate::processing::process_transaction;

// This is a helper function I use in several tests. It isn't called
// in production code, but is absolutely useful, and used.
#[allow(dead_code)]
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

// Also a very useful helper function.
#[allow(dead_code)]
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
                // This is probably wrong, fix <ajm>
                amount: None::<Decimal>,
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
fn test_dispute_nonexistant_transaction() {
    let mut result = get_default_ledger();
    let mut transaction_ledger: HashMap<u32, LedgerEntry> = HashMap::from([]);
    let mut failed_transactions: Vec<(LedgerEntry, TransactionError)> = vec![];

    let record = LedgerEntry {
        tx_type: TransactionType::Dispute,
        client: 1,
        tx: 1,
        amount: None::<Decimal>,
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
        amount: None::<Decimal>,
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
        amount: None::<Decimal>,
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
        amount: None::<Decimal>,
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
        amount: None::<Decimal>,
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
        amount: None::<Decimal>,
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
        amount: None::<Decimal>,
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
        amount: None::<Decimal>,
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

    let mut transaction_ledger: HashMap<u32, LedgerEntry> = HashMap::from([(1, record.clone())]);
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

    let mut transaction_ledger: HashMap<u32, LedgerEntry> = HashMap::from([(1, record.clone())]);
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
        amount: None::<Decimal>,
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

#[test]
fn test_dispute_already_disputed_transaction() {
    let mut result = get_default_ledger();
    let expected_result = get_default_ledger();

    let record = LedgerEntry {
        tx_type: TransactionType::Dispute,
        client: 1,
        tx: 2,
        amount: None::<Decimal>,
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
        amount: None::<Decimal>,
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
        amount: None::<Decimal>,
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
        amount: None::<Decimal>,
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
        amount: None::<Decimal>,
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
        amount: None::<Decimal>,
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
        amount: None::<Decimal>,
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
        amount: None::<Decimal>,
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

#[tokio::test]
async fn test_corrupt_row_behavior() -> Result<()> {
    let ledger = process_transactions("src/bad_parse.csv").await?;
    assert_eq!(
        ledger,
        HashMap::from([(
            1,
            AccountInfo {
                available: dec!(150.00),
                held: dec!(0.0),
                locked: false,
            }
        )])
    );

    Ok(())
}
