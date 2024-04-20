# off-chain-ledger

Hello and welcome! This is a utility project I put together. The
conceit is that you are processing transaction off-chain. The
transaction is represented by a CSV file, where each row is a
transaction. Deposits and Withdrawals have a globally-unique
transaction ID, a client ID to which the transaction applies, and an
amount to be credited or debited to the account.

There are also transactions _about_ transactions, in the form of
Dispute, Resolve, and Chargeback actions. These contain a customer ID
and transaction ID, but not an amount. The expectation is that we
refer to a record of transactions to find the amount in question.

You can call this program via running:

```shell
cargo run your_csv_file.csv
```

After the program is done processing, it will output a CSV table of
each customer's holdings, and account status. It's completely possible
to redirect the program's output to a csv file, like so:

```shell
cargo run your_csv_file.csv > ledger_result.csv
```

Also available is instrumentation of what's happening where via the
`env-logger` crate. You can specify a logging level (currently only
debug, info, and error are used). You can change this by setting the
`RUST_LOG` variable like so:

```shell
RUST_LOG=INFO cargo run -- your_csv_file.csv
```

There's also a number of tests available, some using csv files in
`src/`. `cargo test` will run them for you. Note that two of the tests
involve processing 1m rows of transactions, so tests take a bit longer
to run than you'd usually expect. I may decide to disable those by
default in the future, but having confidence that my program was
correct, even with many operations, was tremendously helpful for me
during development.

This project also uses tokio for async IO, csv_async to serialize and
deserialize CSV data, and decimal to keep track of decimal numbers
with precision. We also use the `anyhow` crate to provide a convenient
`Result<T>` type.

