use std::{
    collections::{HashMap, HashSet},
    env, io,
};

use csv::{ReaderBuilder, Writer};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

fn main() -> Result<(), anyhow::Error> {
    // Type stuff: There's not many record cases, and I'm using serde and enums for parsing, so no case can possibly go unhandled.
    //             It would be nice to get the information of a record having an associated value into the type syatems as well
    //             But the type machinery for that would be too long for this kind of a toy assignment
    // No unit tests, because it's getting late when I'm writing this. No complicated bits though anyway.

    // Clap would be cooler, but also massive for this
    let in_path = env::args().skip(1).take(1).next().expect("No arg");

    // A HashMap would be marginally more readable, but let's go fast and preallocate a big array, because we can with u16 keys and 500kb is nothing
    // Massive overkill for small examples though
    let mut seen_clients: Vec<ClientId> = Vec::with_capacity(1 << 13);
    const NONE: std::option::Option<Box<ClientState>> = None;
    let mut client_states: Box<[Option<Box<ClientState>>; 1 << 16]> = Box::new([NONE; 1 << 16]);

    // Clients are ok for in-memory, but this would probably need disk storage and memory cache for real life applications
    // As this is just a wrapper over a hashmap, so I hope you're not throwing gigabytes of csv at this, because it could OOM easily
    // I'm only saving the transactions that have IDs of their own, so deposit and withdraw, as the others do not (which is insane for real life ofc)
    let mut tx_database = TxDatabase::new();

    let mut csv_reader = ReaderBuilder::new()
        .flexible(true)
        .trim(csv::Trim::All)
        .from_path(in_path)?;

    // Let's have it like this so we could easily change it to a tokio stream if needed
    let transaction_iter: Box<dyn Iterator<Item = Result<TransactionRecord, anyhow::Error>>> =
        Box::new(csv_reader.deserialize().map(
            |result| -> Result<TransactionRecord, anyhow::Error> {
                let record: TransactionRecord = result?;
                Ok(record)
            },
        ));

    handle_transactions(
        transaction_iter,
        &mut client_states,
        &mut seen_clients,
        &mut tx_database,
    )?;

    write_output(seen_clients, &client_states)?;
    Ok(())
}

fn handle_transactions(
    record_iter: impl Iterator<Item = Result<TransactionRecord, anyhow::Error>>,
    client_states: &mut [Option<Box<ClientState>>; 1 << 16],
    seen_clients: &mut Vec<ClientId>,
    tx_database: &mut TxDatabase,
) -> Result<(), anyhow::Error> {
    for record in record_iter {
        let record = record?;

        // For disputes etc we're modifying the client id in the dispute transaction,
        // And not in the original transaction
        // Quite unsafe if we do not trust the data source (but we do)
        let client = &mut client_states[record.client_id as usize];
        // Create a default client if none exists
        if client.is_none() {
            seen_clients.push(record.client_id);
            *client = Some(Box::new(ClientState::new()));
        };
        let client = client.as_deref_mut().unwrap();

        // Handle transaction
        match record.transaction_type {
            TransactionType::Deposit => {
                client.available += record.value.expect("Invalid record");
                // Store transaction for posterity
                tx_database.save(record);
            }
            TransactionType::Withdrawal => {
                let value = record.value.expect("Invalid record");
                if client.available < value {
                    // The spec does not mention if a failed withdrawal is disputable
                    // There's no harm in treating it as such, but it needs to be specified
                    tx_database.save(record);
                    continue;
                }

                client.available -= value;
                tx_database.save(record);
            }
            TransactionType::Dispute => {
                // Note that there's no checking that the dispute belongs to the same client as the transaction, as that was not specified
                let Some(referenced_tx) = tx_database.query(record.transaction_id) else {
                    continue;
                };

                // No logic protects you with disputing the same transaction twice in a row
                // Also not specified
                client
                    .txns_under_dispute
                    .insert(referenced_tx.transaction_id);

                let value = referenced_tx.value.expect("can't happen. It's possible to make the compiler know this, but I'm not duplicating half my types just for that");

                // This doesn't make sense if the disputed transaction is a withdrawal
                // But "decrease" means "decrease", and the spec is the spec
                // Same logic follows for resolving and chargebacks
                client.available -= value;
                client.held += value;
            }
            TransactionType::Resolve => {
                let Some(referenced_tx) = tx_database.query(record.transaction_id) else {
                    continue;
                };
                if !client
                    .txns_under_dispute
                    .contains(&referenced_tx.transaction_id)
                {
                    continue;
                }
                client
                    .txns_under_dispute
                    .remove(&referenced_tx.transaction_id);

                let value = referenced_tx.value.expect("can't happen. It's possible to make the compiler know this, but I'm not duplicating half my types just for that");

                client.held -= value;
                client.available += value;
            }
            TransactionType::Chargeback => {
                let Some(referenced_tx) = tx_database.query(record.transaction_id) else {
                    continue;
                };
                if !client
                    .txns_under_dispute
                    .contains(&referenced_tx.transaction_id)
                {
                    continue;
                }
                client
                    .txns_under_dispute
                    .remove(&referenced_tx.transaction_id);

                let value = referenced_tx.value.expect("can't happen. It's possible to make the compiler know this, but I'm not duplicating half my types just for that");

                client.held -= value;
                // Spec does not mention if an account being frozen blocks future transactions, so I'm not doing that
                client.locked = true;
            }
        }
    }
    Ok(())
}

fn write_output(
    seen_clients: Vec<ClientId>,
    client_states: &[Option<Box<ClientState>>; 1 << 16],
) -> Result<(), anyhow::Error> {
    let mut csv_writer = Writer::from_writer(io::stdout());
    for seen_client_id in seen_clients {
        let client_state = client_states[seen_client_id as usize].as_ref().unwrap();
        csv_writer.serialize(ClientRecord::from_id_and_state(
            &seen_client_id,
            client_state,
        ))?;
    }
    csv_writer.flush()?;
    Ok(())
}

type TransactionId = u32;
type ClientId = u16;

#[derive(Serialize, Debug)]
struct ClientRecord {
    #[serde(rename = "client")]
    client_id: ClientId,
    available: Decimal,
    held: Decimal,
    total: Decimal,
    locked: bool,
}

impl ClientRecord {
    fn from_id_and_state(id: &ClientId, state: &ClientState) -> Self {
        Self {
            client_id: *id,
            available: state.available,
            held: state.held,
            total: state.held + state.available,
            locked: state.locked,
        }
    }
}

#[derive(Debug)]
struct ClientState {
    available: Decimal,
    held: Decimal,
    locked: bool,
    txns_under_dispute: HashSet<TransactionId>,
}

impl ClientState {
    fn new() -> Self {
        Self {
            available: Decimal::default(),
            held: Decimal::default(),
            locked: false,
            txns_under_dispute: HashSet::new(),
        }
    }
}
struct TxDatabase {
    db: HashMap<TransactionId, TransactionRecord>,
}

impl TxDatabase {
    fn new() -> Self {
        Self {
            db: HashMap::with_capacity(4096),
        }
    }
    fn query(&self, tx_id: TransactionId) -> Option<&TransactionRecord> {
        self.db.get(&tx_id)
    }
    fn save(&mut self, record: TransactionRecord) {
        self.db.insert(record.transaction_id, record);
    }
}

#[derive(Deserialize, Debug)]
struct TransactionRecord {
    #[serde(rename = "type")]
    transaction_type: TransactionType,
    #[serde(rename = "client")]
    client_id: ClientId,
    #[serde(rename = "tx")]
    transaction_id: TransactionId,
    // size unspecified in spec, so let's default to rust_decimal
    #[serde(rename = "amount")]
    #[serde(with = "rust_decimal::serde::str_option")]
    value: Option<Decimal>,
}
#[derive(Deserialize, Debug)]
enum TransactionType {
    #[serde(rename = "deposit")]
    Deposit,
    #[serde(rename = "withdrawal")]
    Withdrawal,
    #[serde(rename = "dispute")]
    Dispute,
    #[serde(rename = "resolve")]
    Resolve,
    #[serde(rename = "chargeback")]
    Chargeback,
}
