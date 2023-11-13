use super::clients::{ClientRecord, ClientRecords};
use anyhow::{anyhow, Result};
use aws_sdk_kinesis as kinesis;
use rand::seq::SliceRandom;
use rand::thread_rng;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};

pub struct Config {
    pub kinesis_client: Box<kinesis::Client>,
    pub client_id: String,
}

pub struct Sharder {
    config: Config,
}

impl Sharder {
    pub fn new(c: Config) -> Result<Self> {
        let sharder = Sharder { config: c };

        return Ok(sharder);
    }

    // calculateOwnedShards is effectively a Maglev Consistent Hash
    // reference: https://storage.googleapis.com/pub-tools-public-publication-data/pdf/44824.pdf
    // The main difference is we use the lookup table directly to map shards to clients
    // rather than hashing keys into the lookup table.
    // Maglev Hashing ensures we get near-perfect distribution of shards to clients
    // while also minimizing shard movement when adding or removing shards or clients.
    pub fn calculate_owned_shards(
        &mut self,
        clients: ClientRecords,
        shards: Vec<String>,
    ) -> HashSet<String> {
        let mut lookup_table: Vec<isize> = Vec::new();
        for _ in 0..shards.len() {
            lookup_table.push(-1);
        }

        // Generate a unique permutation of shardIndexes for each client that is random
        // but stable for a given client ID. In the Maglev paper they describe a faster
        // implementation but then state:
        //     Other methods of generating random permutations, such as the Fisher-Yates
        //     Shuffle [20], generate better quality permutations using more state, and
        //     would work fine here as well.
        let mut permutations: Vec<Vec<usize>> = Vec::new();
        for _ in 0..clients.0.len() {
            let mut permutation: Vec<usize> = Vec::new();
            for sidx in 0..shards.len() {
                permutation.push(sidx);
            }

            permutation.shuffle(&mut thread_rng());
            permutations.push(permutation);
        }

        // The bulk of the function: For each client try to claim the next shardIndex in
        // its permuation list. If the shard is already taken, continue down the permuatation
        // until a free shard is found. This ensures each client gets an equal number of shards
        // and that shards are assigned randomly but in a stable manner.
        let mut next: Vec<usize> = Vec::new();
        for _ in 0..clients.0.len() {
            next.push(0);
        }

        let mut assigned = 0;
        loop {
            let mut assigned_all = false;
            for (cidx, _) in clients.0.iter().enumerate() {
                let mut next_val = next[cidx].clone();
                next[cidx] = next_val + 1;

                while lookup_table[permutations[cidx][next_val]] > 0 {
                    next_val = next[cidx].clone();
                    next[cidx] = next_val + 1;
                }

                lookup_table[permutations[cidx][next_val]] = cidx as isize;
                assigned += 1;

                if assigned == shards.len() {
                    assigned_all = true;
                    break;
                }
            }

            if assigned_all {
                break;
            }
        }

        let mut client_position: usize = 0;
        for (cidx, client) in clients.0.iter().enumerate() {
            if self.config.client_id == client.id {
                client_position = cidx;
                break;
            }
        }

        let mut owned_shards: HashSet<String> = HashSet::new();
        for (lidx, value) in lookup_table.iter().enumerate() {
            let cur_position: usize = *value as usize;
            if cur_position == client_position {
                owned_shards.insert(shards[lidx].clone());
            }
        }

        return owned_shards;
    }
}
