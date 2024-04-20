use anyhow::Result;
use std::collections::HashMap;
use tokio::io;

use crate::types::*;

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
pub async fn write_ledger(ledger: &mut HashMap<u16, AccountInfo>) -> Result<()> {
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
