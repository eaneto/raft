use std::{
    cmp,
    collections::{HashMap, HashSet},
    io::{Read, Write},
    net::TcpStream,
    time::{Duration, Instant},
};

use log::{debug, error, info, trace};

use bytes::BytesMut;
use rand::Rng;
use serde::{Deserialize, Serialize};

enum State {
    Leader,
    Follower,
    Candidate,
}

pub struct Server {
    id: NodeId,
    // Need to be stored on disk
    current_term: u64,
    voted_for: Option<NodeId>,
    log: Vec<LogEntry>,
    // Can be stored in-memory
    state: State,
    // commit_index
    commit_length: u64,
    election_timeout: u16,
    current_leader: u64,
    votes_received: HashSet<NodeId>,
    // sent_index
    sent_length: HashMap<NodeId, u64>,
    // match_index
    acked_length: HashMap<NodeId, u64>,
    nodes: HashMap<NodeId, Node>,
    last_heartbeat: Option<Instant>,
}

type NodeId = u64;

#[derive(Clone, Debug)]
pub struct Node {
    pub id: NodeId,
    pub address: String,
}

impl Server {
    #[must_use]
    pub fn new(id: u64, nodes: HashMap<u64, Node>) -> Server {
        Server {
            id,
            current_term: 0,
            voted_for: None,
            log: Vec::new(),
            state: State::Follower,
            commit_length: 0,
            election_timeout: rand::thread_rng().gen_range(150..300),
            current_leader: 0,
            votes_received: HashSet::new(),
            sent_length: HashMap::new(),
            acked_length: HashMap::new(),
            nodes,
            last_heartbeat: None,
        }
    }

    #[must_use]
    fn current_term(&self) -> u64 {
        self.current_term
    }

    fn increment_current_term(&mut self) {
        self.current_term += 1;
    }

    fn decrement_current_term(&mut self) {
        self.current_term -= 1;
    }

    fn update_current_term(&mut self, value: u64) {
        self.current_term = value;
    }

    fn is_candidate(&self) -> bool {
        matches!(self.state, State::Candidate)
    }

    #[must_use]
    pub fn is_leader(&self) -> bool {
        matches!(self.state, State::Leader)
    }

    fn become_leader(&mut self) {
        self.state = State::Leader;
    }

    fn become_follower(&mut self) {
        self.state = State::Follower;
    }

    fn become_candidate(&mut self) {
        self.state = State::Candidate;
    }

    #[must_use]
    fn commit_length(&self) -> u64 {
        self.commit_length
    }

    fn increment_commit_length(&mut self) {
        self.commit_length += 1;
    }

    fn update_commit_length(&mut self, value: u64) {
        self.commit_length = value;
    }

    #[must_use]
    pub fn election_timeout(&self) -> u16 {
        self.election_timeout
    }

    fn vote_on_self(&mut self) {
        self.votes_received.insert(self.id);
    }

    fn unvote(&mut self) {
        self.voted_for = None;
        self.votes_received.clear();
    }

    fn vote_on_new_leader(&mut self, node: u64) {
        self.votes_received.insert(node);
    }

    fn update_sent_length(&mut self, node_id: NodeId, length: u64) {
        self.sent_length.insert(node_id, length);
    }

    fn decrement_sent_length(&mut self, node_id: NodeId) {
        let sent_length_by_node = self.sent_length.get(&node_id).unwrap();
        self.sent_length.insert(node_id, sent_length_by_node - 1);
    }

    #[must_use]
    pub fn last_heartbeat(&self) -> Option<Instant> {
        self.last_heartbeat
    }

    fn update_last_heartbeat(&mut self) {
        self.last_heartbeat = Some(Instant::now());
    }

    fn log_length(&self) -> usize {
        self.log.len()
    }

    /// Checks if the last heartbeat received from the leader(if there
    /// is a last heartbeat) has passed the election timeout.
    #[must_use]
    pub fn no_hearbeats_received_from_leader(&self) -> bool {
        let election_timeout = Duration::from_millis(u64::from(self.election_timeout()));
        let time_elapsed = match Instant::now().checked_sub(election_timeout) {
            Some(time) => time,
            None => Instant::now(),
        };
        let last_heartbeat = self.last_heartbeat();
        (last_heartbeat
            .is_some_and(|heartbeat| time_elapsed.duration_since(heartbeat) > election_timeout)
            || last_heartbeat.is_none())
            && !self.is_leader()
    }

    pub fn start_election(&mut self) {
        if self.is_leader() {
            return;
        }

        if !self.is_candidate() {
            self.increment_current_term();
            self.become_candidate();
            self.voted_for = Some(self.id);
            self.vote_on_self();
        }

        let last_term = self.last_term();

        let vote_request = VoteRequest {
            node_id: self.id,
            current_term: self.current_term(),
            log_length: 0,
            last_term,
        };

        let nodes = self.nodes.clone();
        for node in nodes.values() {
            // TODO Send requests in parallel.
            // TODO Treat error
            trace!("Sending vote request to {}", &node.id);
            if let Ok(response) = Self::send_vote_request(&vote_request, node) {
                self.process_vote_response(response);
            }
        }

        if self.is_candidate() {
            self.decrement_current_term();
            self.become_follower();
            self.unvote();
        }
    }

    fn send_vote_request<'this>(
        vote_request: &'this VoteRequest,
        node: &'this Node,
    ) -> Result<VoteResponse, &'this str> {
        let command_byte = 4_u8.to_be_bytes();
        let encoded_request = bincode::serialize(vote_request).unwrap();
        let mut buf = Vec::new();
        buf.extend(command_byte);
        buf.extend(encoded_request.len().to_be_bytes());
        buf.extend(encoded_request);

        let mut retries: u8 = 0;
        let stream = loop {
            if let Ok(stream) = TcpStream::connect(&node.address) {
                break Some(stream);
            }
            if retries >= 3 {
                break None;
            }
            retries += 1;
        };

        let Some(mut stream) = stream else {
            error!("Can't connect to node at {}", &node.address);
            return Err("Can't connect to node");
        };

        // What should be done in case of failure?
        if stream.write_all(&buf).is_err() {
            trace!("Successfully sent request to node {}", &node.id);
        } else {
            error!("Unable to send request to node {}", &node.id);
            return Err("Unable to send request to node");
        }
        let mut buf = [0; 1024];
        let Ok(_) = stream.read(&mut buf) else {
            error!("Can't read response from client {}", &node.id);
            return Err("Can't read response from client");
        };
        if buf[0] == 0 {
            let length = buf.get(1..9).unwrap();
            let length = usize::from_be_bytes(length.try_into().unwrap());
            let Some(encoded_response) = buf.get(9..(9 + length)) else {
                return Err("Incomplete response, unable to parse vote response");
            };
            let Ok(response) = bincode::deserialize(encoded_response) else {
                return Err("Unable to deserialize server response");
            };
            Ok(response)
        } else {
            Err("Response is not successful")
        }
    }

    fn process_vote_response(&mut self, vote_response: VoteResponse) {
        if vote_response.term > self.current_term() {
            trace!("Vote response term is higher than current term, becoming follower");
            self.update_current_term(vote_response.term);
            self.become_follower();
            self.unvote();
            // TODO: Cancel election timer
        } else if self.is_candidate()
            && vote_response.term == self.current_term()
            && vote_response.vote_in_favor
        {
            self.vote_on_new_leader(vote_response.node_id);
            trace!("Received vote in favor from {}", &vote_response.node_id);
            if self.has_majority_of_votes() {
                info!("Majority of votes in favor received, becoming leader");
                self.become_leader();
                self.current_leader = self.id;
                // TODO: Cancel election timer
                let nodes = self.nodes.clone();
                for node in nodes.values() {
                    self.update_sent_length(node.id, self.log.len() as u64);
                    self.acked_length.insert(node.id, 0);
                    // FIXME
                    let _ = self.replicate_log(node);
                }
            }
        }
    }

    fn has_majority_of_votes(&self) -> bool {
        // Each server has a list of other nodes in the cluster, so
        // the number of nodes is the cluster is the current number of
        // nodes + 1(itself).
        let nodes_on_cluster = self.nodes.len() + 1;
        // Increments the number of nodes on cluster for dividing up,
        // e.g. if there are 5 nodes, the majority must be 3, without
        // the increment diving five by two will result in 2 instead
        // of 3.
        let majority = (nodes_on_cluster + 1) / 2;
        let votes_received = self.votes_received.len();
        votes_received >= majority
    }

    pub fn receive_vote(&mut self, vote_request: VoteRequest) -> VoteResponse {
        if vote_request.current_term > self.current_term() {
            self.update_current_term(vote_request.current_term);
            self.become_follower();
            self.unvote();
        }
        let last_term = self.last_term();
        let ok = (vote_request.last_term > last_term)
            || (vote_request.last_term == last_term
                && vote_request.log_length >= self.log.len() as u64);
        if vote_request.current_term == self.current_term()
            && ok
            && (self.voted_for.is_none() || self.voted_for == Some(vote_request.node_id))
        {
            self.voted_for = Some(vote_request.node_id);
            VoteResponse {
                node_id: self.id,
                term: self.current_term(),
                vote_in_favor: true,
            }
        } else {
            VoteResponse {
                node_id: self.id,
                term: self.current_term(),
                vote_in_favor: false,
            }
        }
    }

    fn last_term(&self) -> u64 {
        if self.log.is_empty() {
            0
        } else {
            self.log[self.log.len() - 1].term
        }
    }

    // Log replication

    pub fn broadcast_message(&mut self, message: BytesMut) {
        if self.is_leader() {
            self.log.push(LogEntry {
                term: self.current_term(),
                message,
            });
            self.acked_length.insert(self.id, self.current_term());
            self.broadcast_current_log();
        } else {
            unimplemented!("Leader forwarding not implemented yet")
        }
    }

    pub fn broadcast_current_log(&mut self) {
        if self.is_leader() {
            trace!("Starting log broadcast");
            let nodes = self.nodes.clone();
            for node in nodes.values() {
                if let Ok(response) = self.replicate_log(node) {
                    self.process_log_response(response);
                }
            }
        }
    }

    // Can only be called by the leader
    fn replicate_log(&self, node: &Node) -> Result<LogResponse, &str> {
        let request = if let Some(entry) = self.sent_length.get(&node.id) {
            let prefix_length = usize::try_from(*entry).unwrap_or_default();
            let suffix = &self.log[prefix_length..];
            let prefix_term = if prefix_length > 0 {
                self.log[prefix_length - 1].term
            } else {
                0
            };
            LogRequest {
                leader_id: self.id,
                term: self.current_term(),
                prefix_length,
                prefix_term,
                leader_commit: self.commit_length(),
                suffix: suffix.to_vec(),
            }
        } else {
            // If there are logs sent just send an empty vector as a
            // heartbeat.
            debug!("Sending heartbeat to replicas");
            LogRequest {
                leader_id: self.id,
                term: self.current_term(),
                prefix_length: 0,
                prefix_term: 0,
                leader_commit: self.commit_length(),
                suffix: Vec::new(),
            }
        };

        self.send_log_request(node, &request)
    }

    // TODO: Handle errors betters, create different error types.
    fn send_log_request(&self, node: &Node, log_request: &LogRequest) -> Result<LogResponse, &str> {
        let command_byte = 5_u8.to_be_bytes();
        let encoded_request = bincode::serialize(log_request).unwrap();
        let mut buf = Vec::new();
        buf.extend(command_byte);
        buf.extend(encoded_request.len().to_be_bytes());
        buf.extend(encoded_request);

        let mut retries: u8 = 0;
        let stream = loop {
            if let Ok(stream) = TcpStream::connect(&node.address) {
                break Some(stream);
            }
            if retries >= 3 {
                break None;
            }
            retries += 1;
        };

        let Some(mut stream) = stream else {
            error!("Can't connect to node at {}", &node.address);
            return Err("Can't connect to node");
        };

        // What should be done in case of failure?
        if stream.write_all(&buf).is_ok() {
            trace!("Successfully sent request to node {}", &node.id);
        } else {
            error!("Unable to send request to node {}", &node.id);
            return Err("Unable to send request to node");
        };
        let mut buf = [0; 1024];
        let Ok(_) = stream.read(&mut buf) else {
            error!("Can't read response from client {}", &node.id);
            return Err("Can't read response from client");
        };
        if buf[0] == 0 {
            let length = buf.get(1..9).unwrap();
            let length = usize::from_be_bytes(length.try_into().unwrap());
            let Some(encoded_response) = buf.get(9..(9 + length)) else {
                error!(
                    "Incomplete response, unable to parse log response from client {}",
                    &node.id
                );
                return Err("Incomplete response, unable to parse log response");
            };
            let Ok(response) = bincode::deserialize(encoded_response) else {
                error!(
                    "Unable to deserialize server response from client {}",
                    &node.id
                );
                return Err("Unable to deserialize server response");
            };
            trace!("Received successful response from {}", &node.id);
            Ok(response)
        } else {
            trace!("Received failed response from {}", &node.id);
            Err("Response is not successful")
        }
    }

    fn process_log_response(&mut self, log_response: LogResponse) {
        if log_response.term > self.current_term() {
            self.update_current_term(log_response.term);
            self.become_follower();
            self.unvote();
            // TODO: Cancel election timer
            return;
        }

        if log_response.term == self.current_term() && self.is_leader() {
            if log_response.successful
                && &log_response.ack >= self.acked_length.get(&log_response.node_id).unwrap()
            {
                self.update_sent_length(log_response.node_id, log_response.ack);
                self.acked_length
                    .insert(log_response.node_id, log_response.ack);
                self.commit_log_entries();
            } else if *self.sent_length.get(&log_response.node_id).unwrap() > 0 {
                self.decrement_sent_length(log_response.node_id);

                let node = self.nodes.get(&log_response.node_id).unwrap();
                // TODO: ?
                let _ = self.replicate_log(node);
            }
        }
    }

    pub fn receive_log_request(&mut self, log_request: &LogRequest) -> LogResponse {
        self.update_last_heartbeat();
        if log_request.term > self.current_term() {
            self.update_current_term(log_request.term);
            self.unvote();
            // TODO: Cancel election timer
        }
        // TODO: Is this the correct condition? Should the state be
        // modified every single time?
        if log_request.term == self.current_term() {
            self.become_follower();
            self.current_leader = log_request.leader_id;
        }

        let ok = (self.log_length() >= log_request.prefix_length)
            && (log_request.prefix_length == 0
                || self.log[log_request.prefix_length - 1].term == log_request.prefix_term);
        if log_request.term == self.current_term() && ok {
            let ack = log_request.prefix_length + log_request.suffix.len();
            self.send_append_entries(log_request);
            let response = LogResponse {
                node_id: self.id,
                term: self.current_term(),
                ack: ack as u64,
                successful: true,
            };
            trace!("Sending log response {:?}", response);
            response
        } else {
            let response = LogResponse {
                node_id: self.id,
                term: self.current_term(),
                ack: 0,
                successful: false,
            };
            trace!("Sending log response {:?}", response);
            response
        }
    }

    fn commit_log_entries(&mut self) {
        while self.commit_length() < self.log_length() as u64 {
            let mut acks = 0;
            let nodes = self.nodes.clone();
            for node in nodes.values() {
                let acked_length = *self.acked_length.get(&node.id).unwrap();
                if acked_length > self.commit_length() {
                    acks += 1;
                }
            }

            if acks >= (self.nodes.len() + 1) / 2 {
                // TODO: Deliver log
                self.increment_commit_length();
            } else {
                // No consensus
                break;
            }
        }
    }

    fn send_append_entries(&mut self, log_request: &LogRequest) {
        let log_length = self.log_length();
        if !log_request.suffix.is_empty() && log_length > log_request.prefix_length {
            let index = cmp::min(
                log_length,
                log_request.prefix_length + log_request.suffix.len(),
            ) - 1;
            // Log is inconsistent
            if self.log[index].term != log_request.suffix[index - log_request.prefix_length].term {
                self.log = self.log[..log_request.prefix_length - 1].to_vec();
            }
        }

        if log_request.prefix_length + log_request.suffix.len() > log_length {
            let start = log_length - log_request.prefix_length;
            let end = log_request.suffix.len() - 1;
            for i in start..end {
                self.log.push(log_request.suffix[i].clone());
            }
        }

        if log_request.leader_commit > self.commit_length() {
            for _ in (self.commit_length())..(log_request.leader_commit - 1) {
                // TODO
            }
            self.update_commit_length(log_request.leader_commit);
        }
    }
}

// RPC
#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub struct VoteRequest {
    pub node_id: u64,
    pub current_term: u64,
    pub log_length: u64,
    pub last_term: u64,
}

#[derive(Serialize, Deserialize, Copy, Clone)]
pub struct VoteResponse {
    node_id: u64,
    term: u64,
    vote_in_favor: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LogEntry {
    pub term: u64,
    pub message: BytesMut,
}

// RPC append entries
#[derive(Serialize, Deserialize)]
pub struct LogRequest {
    pub leader_id: u64,
    pub term: u64,
    pub prefix_length: usize,
    pub prefix_term: u64,
    pub leader_commit: u64,
    pub suffix: Vec<LogEntry>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub struct LogResponse {
    node_id: u64,
    term: u64,
    ack: u64,
    successful: bool,
}

#[cfg(test)]
mod tests {

    use crate::raft::*;

    #[test]
    fn should_start_a_new_election_with_no_hearbeats() {
        let server = Server::new(1, HashMap::new());

        let result = server.no_hearbeats_received_from_leader();

        assert!(result);
    }

    #[test]
    fn should_start_a_new_election_with_outdated_hearbeats() {
        let one_second_ago = Instant::now() - Duration::from_secs(1);
        let server = Server {
            id: 1,
            current_term: 0,
            voted_for: None,
            log: Vec::new(),
            state: State::Follower,
            commit_length: 0,
            election_timeout: rand::thread_rng().gen_range(150..300),
            current_leader: 0,
            votes_received: HashSet::new(),
            sent_length: HashMap::new(),
            acked_length: HashMap::new(),
            nodes: HashMap::new(),
            last_heartbeat: Option::Some(one_second_ago),
        };

        let result = server.no_hearbeats_received_from_leader();

        assert!(result);
    }

    #[test]
    fn should_not_start_a_new_election() {
        let server = Server {
            id: 1,
            current_term: 0,
            voted_for: None,
            log: Vec::new(),
            state: State::Follower,
            commit_length: 0,
            election_timeout: rand::thread_rng().gen_range(150..300),
            current_leader: 0,
            votes_received: HashSet::new(),
            sent_length: HashMap::new(),
            acked_length: HashMap::new(),
            nodes: HashMap::new(),
            last_heartbeat: Option::Some(Instant::now()),
        };

        let result = server.no_hearbeats_received_from_leader();

        assert!(!result);
    }
}
