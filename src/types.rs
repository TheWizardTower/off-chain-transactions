use rust_decimal::Decimal;
use rust_decimal_macros::dec;

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
pub struct AccountInfo {
    pub available: Decimal, // Available for use.
    pub held: Decimal,      // Held because of a disputed charge.
    pub locked: bool,       // Has been locked because of a chargeback.
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
pub struct AccountInfoWithTotal {
    pub client: u16,
    pub available: Decimal,
    pub held: Decimal,
    pub total: Decimal,
    pub locked: bool,
}

// Create a sum type for the types of transactions we can process.
// We're also doing some serde footwork to make it so we get nice
// Rust-format names while still accepting 'deposit' and so forth in
// the actual CSV files.
#[derive(Clone, Debug, serde::Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum TransactionType {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    Chargeback,
}

#[derive(Clone, Debug, PartialEq)]
pub enum TransactionStatus {
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
pub struct LedgerEntry {
    #[serde(rename(serialize = "type", deserialize = "type"))]
    pub tx_type: TransactionType, // Transaction Type. Will be with header name 'type', which is unfortunately a reserved word in rust.
    pub client: u16,             // Client ID
    pub tx: u32,                 // Transaction ID
    pub amount: Option<Decimal>, // At least four decimal places of precision is expected. Not present on every type of transaction.
    #[serde(skip)]
    #[serde(default = "disputed_default")]
    pub disputed_status: TransactionStatus,
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
pub enum TransactionError {
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
