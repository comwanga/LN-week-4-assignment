use bitcoincore_rpc::bitcoin::{Address, Amount, Network};
use bitcoincore_rpc::{Auth, Client as BitcoinClient, RpcApi};
use reqwest::blocking::Client;
use serde_json::{json, Value};
use std::str::FromStr;
use std::thread::sleep;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

const BITCOIN_RPC_URL: &str = "http://localhost:18443";
const MINING_WALLET: &str = "mining_wallet";
const ALICE_BOB_CAPACITY_SAT: u64 = 500_000;
const BOB_CAROL_CAPACITY_SAT: u64 = 300_000;
const CAROL_ALICE_CAPACITY_SAT: u64 = 400_000;
const REBALANCE_MSAT: u64 = 150_000_000;
const INVOICE_DESCRIPTION: &str = "Circular Rebalance";
const OUTPUT_FILE: &str = "../out.txt";

/// Call Alice's Lightning node via CLN REST API on port 3010
fn call_alice_ln(method: &str, params: Value) -> Result<Value> {
    let rune = std::env::var("ALICE_RUNE")?;
    let url = format!("http://localhost:3010/v1/{}", method);

    let client = Client::new();
    let response = client
        .post(&url)
        .json(&params)
        .header("Rune", rune)
        .send()?
        .json::<Value>()?;

    Ok(response)
}

/// Call Bob's Lightning node via CLN REST API on port 3011
fn call_bob_ln(method: &str, params: Value) -> Result<Value> {
    let rune = std::env::var("BOB_RUNE")?;
    let url = format!("http://localhost:3011/v1/{}", method);

    let client = Client::new();
    let response = client
        .post(&url)
        .json(&params)
        .header("Rune", rune)
        .send()?
        .json::<Value>()?;

    Ok(response)
}

/// Call Carols's Lightning node via CLN REST API on port 3012
fn call_carol_ln(method: &str, params: Value) -> Result<Value> {
    let rune = std::env::var("CAROL_RUNE")?;
    let url = format!("http://localhost:3012/v1/{}", method);

    let client = Client::new();
    let response = client
        .post(&url)
        .json(&params)
        .header("Rune", rune)
        .send()?
        .json::<Value>()?;

    Ok(response)
}

/// The three Lightning nodes in the ring
#[derive(Clone, Copy, Debug)]
enum Node {
    Alice,
    Bob,
    Carol,
}

impl Node {
    /// Call an RPC method on this node, turning CLN error objects into Rust errors
    fn call(self, method: &str, params: Value) -> Result<Value> {
        let response = match self {
            Node::Alice => call_alice_ln(method, params)?,
            Node::Bob => call_bob_ln(method, params)?,
            Node::Carol => call_carol_ln(method, params)?,
        };
        if let (Some(code), Some(message)) = (response.get("code"), response.get("message")) {
            return Err(format!("{:?} {} failed ({}): {}", self, method, code, message).into());
        }
        Ok(response)
    }

    /// Fetch this node's channel with the given channel_id
    fn channel(self, channel_id: &str) -> Result<Value> {
        self.call("listpeerchannels", json!({}))?["channels"]
            .as_array()
            .and_then(|channels| channels.iter().find(|c| c["channel_id"] == channel_id))
            .cloned()
            .ok_or_else(|| format!("{:?} has no channel {}", self, channel_id).into())
    }

    /// Local balance (msat) this node holds in the given channel
    fn local_balance(self, channel_id: &str) -> Result<u64> {
        msat(&self.channel(channel_id)?["to_us_msat"])
    }
}

/// A channel opened by one node to a peer, tracked by its channel_id
struct Channel {
    opener: Node,
    channel_id: String,
}

/// Routing policy a node advertises for forwarding over one of its channels
struct Policy {
    base_fee_msat: u64,
    fee_ppm: u64,
    cltv_delta: u64,
}

impl Policy {
    /// Fee charged to forward `amount_msat` over this channel
    fn fee(&self, amount_msat: u64) -> u64 {
        self.base_fee_msat + amount_msat * self.fee_ppm / 1_000_000
    }
}

/// Read a msat amount, which CLN encodes as an integer
fn msat(value: &Value) -> Result<u64> {
    value
        .as_u64()
        .ok_or_else(|| format!("expected msat integer, got {}", value).into())
}

/// Read a string field from a JSON value
fn string(value: &Value, field: &str) -> Result<String> {
    value[field]
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| format!("missing string field '{}' in {}", field, value).into())
}

/// Poll `check` once a second until it returns true or `timeout_secs` elapse
fn wait_until(what: &str, timeout_secs: u64, mut check: impl FnMut() -> Result<bool>) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(timeout_secs);
    while !check()? {
        if Instant::now() >= deadline {
            return Err(format!("timed out waiting for {}", what).into());
        }
        sleep(Duration::from_secs(1));
    }
    Ok(())
}

/// Mine `blocks` blocks paying the coinbase to `address`
fn mine(wallet: &BitcoinClient, blocks: u64, address: &Address) -> Result<()> {
    wallet.generate_to_address(blocks, address)?;
    Ok(())
}

/// Ask a Lightning node for a fresh on-chain address and send it `btc` from the mining wallet
fn fund_node(wallet: &BitcoinClient, node: Node, btc: f64) -> Result<()> {
    let address = string(&node.call("newaddr", json!({}))?, "bech32")?;
    let address = Address::from_str(&address)?.require_network(Network::Regtest)?;
    let txid = wallet.send_to_address(&address, Amount::from_btc(btc)?, None, None, None, None, None, None)?;
    println!("Funded {:?} at {} in tx {}", node, address, txid);
    Ok(())
}

/// Look up `source`'s advertised policy for a channel from Alice's gossip view
fn channel_policy(scid: &str, source: &str) -> Result<Option<Policy>> {
    let channels = Node::Alice.call("listchannels", json!({ "short_channel_id": scid }))?;
    let update = channels["channels"]
        .as_array()
        .and_then(|updates| updates.iter().find(|c| c["source"] == source));
    Ok(match update {
        Some(c) => Some(Policy {
            base_fee_msat: msat(&c["base_fee_millisatoshi"])?,
            fee_ppm: msat(&c["fee_per_millionth"])?,
            cltv_delta: msat(&c["delay"])?,
        }),
        None => None,
    })
}

/// Wait until Alice has received gossip for a channel direction and return its policy
fn wait_for_policy(scid: &str, source: &str) -> Result<Policy> {
    let mut policy = None;
    wait_until(&format!("gossip for channel {}", scid), 180, || {
        policy = channel_policy(scid, source)?;
        Ok(policy.is_some())
    })?;
    Ok(policy.expect("policy is set once wait_until succeeds"))
}

fn main() -> Result<()> {
    // Get blockchain info
    let rpc = BitcoinClient::new(
        BITCOIN_RPC_URL,
        Auth::UserPass("alice".to_string(), "password".to_string()),
    )?;

    println!("Blockchain Info: {:?}", rpc.get_blockchain_info()?);
    // Get Alice's node info
    let alice_info = Node::Alice.call("getinfo", json!({}))?;
    println!("Alice Node Info: {:?}", alice_info);

    // Get Bob's node info
    let bob_info = Node::Bob.call("getinfo", json!({}))?;
    println!("Bob Node Info: {:?}", bob_info);

    // Get Carol's node info
    let carol_info = Node::Carol.call("getinfo", json!({}))?;
    println!("Carol Node Info: {:?}", carol_info);

    // Create a bitcoin wallet named 'mining_wallet' if it doesn't exist
    if !rpc.list_wallets()?.iter().any(|w| w == MINING_WALLET) {
        if rpc.create_wallet(MINING_WALLET, None, None, None, None).is_err() {
            // The wallet already exists on disk but isn't loaded yet
            rpc.load_wallet(MINING_WALLET)?;
        }
    }
    let wallet = BitcoinClient::new(
        &format!("{}/wallet/{}", BITCOIN_RPC_URL, MINING_WALLET),
        Auth::UserPass("alice".to_string(), "password".to_string()),
    )?;

    // Generate a mining address and mine initial blocks (101 so the first coinbase matures)
    let mining_address = wallet.get_new_address(None, None)?.require_network(Network::Regtest)?;
    mine(&wallet, 101, &mining_address)?;
    println!("Mining wallet balance: {}", wallet.get_balance(None, None)?);

    // Create and fund an on-chain address for Alice
    fund_node(&wallet, Node::Alice, 1.0)?;

    // Create and fund an on-chain address for Bob
    fund_node(&wallet, Node::Bob, 1.0)?;

    // Create and fund an on-chain address for Carol
    fund_node(&wallet, Node::Carol, 1.0)?;

    // Mine blocks to confirm funding transactions
    mine(&wallet, 6, &mining_address)?;

    // Verify on-chain balance for Alice, Bob, and Carol once each node has caught up to the tip
    let tip = rpc.get_block_count()?;
    for node in [Node::Alice, Node::Bob, Node::Carol] {
        wait_until(&format!("{:?} to see confirmed funds", node), 60, || {
            let synced = node.call("getinfo", json!({}))?["blockheight"].as_u64() >= Some(tip);
            let funds = node.call("listfunds", json!({}))?;
            let confirmed: u64 = funds["outputs"]
                .as_array()
                .map(|outputs| {
                    outputs
                        .iter()
                        .filter(|o| o["status"] == "confirmed")
                        .filter_map(|o| o["amount_msat"].as_u64())
                        .sum()
                })
                .unwrap_or(0);
            if synced && confirmed > 0 {
                println!("{:?} on-chain balance: {} msat", node, confirmed);
            }
            Ok(synced && confirmed > 0)
        })?;
    }

    // Get node IDs for Alice, Bob, and Carol
    let alice_id = string(&alice_info, "id")?;
    let bob_id = string(&bob_info, "id")?;
    let carol_id = string(&carol_info, "id")?;

    // Connect them as peers (container names resolve on the docker network)
    Node::Alice.call("connect", json!({ "id": format!("{}@bob:9735", bob_id) }))?;
    Node::Bob.call("connect", json!({ "id": format!("{}@carol:9735", carol_id) }))?;
    Node::Carol.call("connect", json!({ "id": format!("{}@alice:9735", alice_id) }))?;

    let open_channel = |opener: Node, peer_id: &str, amount_sat: u64| -> Result<Channel> {
        let result = opener.call("fundchannel", json!({ "id": peer_id, "amount": amount_sat }))?;
        println!("{:?} opened channel in tx {}", opener, string(&result, "txid")?);
        Ok(Channel { opener, channel_id: string(&result, "channel_id")? })
    };

    // Alice opens a 500,000 sat channel with Bob
    let alice_bob = open_channel(Node::Alice, &bob_id, ALICE_BOB_CAPACITY_SAT)?;

    // Bob opens a 300,000 sat channel with Carol
    let bob_carol = open_channel(Node::Bob, &carol_id, BOB_CAROL_CAPACITY_SAT)?;

    // Carol opens a 400,000 sat channel with Alice
    let carol_alice = open_channel(Node::Carol, &alice_id, CAROL_ALICE_CAPACITY_SAT)?;

    // Mine at least 6 blocks to confirm channels (6 is also the gossip announcement depth)
    mine(&wallet, 6, &mining_address)?;

    // Wait for channels to reach CHANNELD_NORMAL state
    let ring = [&alice_bob, &bob_carol, &carol_alice];
    for channel in ring {
        wait_until(&format!("{:?}'s channel to be normal", channel.opener), 120, || {
            Ok(channel.opener.channel(&channel.channel_id)?["state"] == "CHANNELD_NORMAL")
        })?;
    }
    let scid = |channel: &Channel| -> Result<String> {
        string(&channel.opener.channel(&channel.channel_id)?, "short_channel_id")
    };
    let (alice_bob_scid, bob_carol_scid, carol_alice_scid) =
        (scid(&alice_bob)?, scid(&bob_carol)?, scid(&carol_alice)?);

    // Check opener's local balance for all channels
    let mut before = Vec::new();
    for channel in ring {
        before.push(channel.opener.local_balance(&channel.channel_id)?);
    }
    println!("Balances before rebalance (msat): {:?}", before);

    // Alice creates a BOLT11 invoice for 150,000 sats and description "Circular Rebalance"
    let label = format!(
        "circular-rebalance-{}",
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis()
    );
    let invoice = Node::Alice.call(
        "invoice",
        json!({ "amount_msat": REBALANCE_MSAT, "label": label, "description": INVOICE_DESCRIPTION }),
    )?;

    // Extract the BOLT11 string and payment hash from the invoice
    let bolt11 = string(&invoice, "bolt11")?;
    let payment_hash = string(&invoice, "payment_hash")?;
    let payment_secret = string(&invoice, "payment_secret")?;
    let decoded = Node::Alice.call("decodepay", json!({ "bolt11": bolt11 }))?;
    let final_cltv = msat(&decoded["min_final_cltv_expiry"])?;

    // Alice pays her own invoice. `pay` rejects self-payments, so build the
    // Alice -> Bob -> Carol -> Alice route by hand and send it with `sendpay`.
    // Pricing the route needs Bob's and Carol's forwarding policies from gossip.
    let bob_policy = wait_for_policy(&bob_carol_scid, &bob_id)?;
    let carol_policy = wait_for_policy(&carol_alice_scid, &carol_id)?;

    // Work backwards from the destination: each hop must receive what it
    // forwards plus its fee, with a CLTV padded by its delay
    let to_alice_amount = REBALANCE_MSAT;
    let to_alice_delay = final_cltv;
    let to_carol_amount = to_alice_amount + carol_policy.fee(to_alice_amount);
    let to_carol_delay = to_alice_delay + carol_policy.cltv_delta;
    let to_bob_amount = to_carol_amount + bob_policy.fee(to_carol_amount);
    let to_bob_delay = to_carol_delay + bob_policy.cltv_delta;

    let route = json!([
        { "id": bob_id, "channel": alice_bob_scid, "amount_msat": to_bob_amount, "delay": to_bob_delay },
        { "id": carol_id, "channel": bob_carol_scid, "amount_msat": to_carol_amount, "delay": to_carol_delay },
        { "id": alice_id, "channel": carol_alice_scid, "amount_msat": to_alice_amount, "delay": to_alice_delay },
    ]);
    println!("Route: {}", route);

    Node::Alice.call(
        "sendpay",
        json!({
            "route": route,
            "payment_hash": payment_hash,
            "payment_secret": payment_secret,
            "bolt11": bolt11,
            "amount_msat": REBALANCE_MSAT,
        }),
    )?;

    // Extract payment preimage and status, polling until the payment resolves
    let mut payment = Value::Null;
    wait_until("payment to resolve", 60, || {
        let pays = Node::Alice.call("listsendpays", json!({ "payment_hash": payment_hash }))?;
        payment = pays["payments"][0].clone();
        Ok(payment["status"] != "pending")
    })?;
    let status = string(&payment, "status")?;
    if status != "complete" {
        return Err(format!("payment did not complete, status: {}", status).into());
    }
    let payment_preimage = string(&payment, "payment_preimage")?;
    println!("Payment {} with preimage {}", status, payment_preimage);

    // Check local balances for all channels once every hop has settled its HTLC
    let mut after = Vec::new();
    for channel in ring {
        wait_until(&format!("{:?}'s HTLCs to settle", channel.opener), 60, || {
            let htlcs = &channel.opener.channel(&channel.channel_id)?["htlcs"];
            Ok(htlcs.as_array().map_or(true, Vec::is_empty))
        })?;
        after.push(channel.opener.local_balance(&channel.channel_id)?);
    }
    println!("Balances after rebalance (msat): {:?}", after);

    // Write to out.txt:
    // - Payment Hash
    // - Payment Preimage
    // - BOLT11 Invoice
    // - local balance between Alice and Bob before CR in msat
    // - local balance between Alice and Bob after CR in msat
    // - local balance between Bob and Carol before CR in msat
    // - local balance between Bob and Carol after CR in msat
    // - local balance between Carol and Alice before CR in msat
    // - local balance between Carol and Alice after CR in msat
    let mut lines = vec![payment_hash, payment_preimage, bolt11];
    for (before, after) in before.iter().zip(&after) {
        lines.push(before.to_string());
        lines.push(after.to_string());
    }
    std::fs::write(OUTPUT_FILE, lines.join("\n") + "\n")?;
    println!("Wrote {}", OUTPUT_FILE);

    Ok(())
}
