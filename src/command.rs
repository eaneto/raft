use crate::raft;
use bincode;

const REQUEST_VOTE_COMMAND_BYTE: u8 = 4;
const LOG_REQUEST_COMMAND_BYTE: u8 = 5;
const START_ELECTION_COMMAND_BYTE: u8 = 6;

#[derive(Debug, PartialEq)]
pub enum Command {
    RequestVote {
        node_id: u64,
        current_term: u64,
        log_length: u64,
        last_term: u64,
    },
    LogRequest {
        leader_id: u64,
        term: u64,
        prefix_length: usize,
        prefix_term: u64,
        leader_commit: u64,
        suffix: Vec<raft::LogEntry>,
    },
    StartElection,
    Unknown,
}

/// Parses a command.
///
/// # Errors
///
/// If the command is unparseable, an error is returned.
pub fn parse(buf: &[u8]) -> Result<Command, &str> {
    let command_byte = match buf.first() {
        Some(command_byte) => *command_byte,
        None => return Err("Unable to parse command byte"),
    };

    match command_byte {
        REQUEST_VOTE_COMMAND_BYTE => parse_request_vote_command(buf),
        LOG_REQUEST_COMMAND_BYTE => parse_log_request_command(buf),
        START_ELECTION_COMMAND_BYTE => Ok(Command::StartElection),
        _ => Ok(Command::Unknown),
    }
}

fn parse_request_vote_command(buf: &[u8]) -> Result<Command, &str> {
    let Some(length) = buf.get(1..9) else {
        return Err("Unparseable command, unable to parse length");
    };

    let length = usize::from_be_bytes(length.try_into().unwrap());
    let vote_request_buffer = buf.get(9..(9 + length)).unwrap();
    let vote_request: raft::VoteRequest = bincode::deserialize(vote_request_buffer).unwrap();

    Ok(Command::RequestVote {
        node_id: vote_request.node_id,
        current_term: vote_request.current_term,
        log_length: vote_request.log_length,
        last_term: vote_request.last_term,
    })
}

fn parse_log_request_command(buf: &[u8]) -> Result<Command, &str> {
    let Some(length) = buf.get(1..9) else {
        return Err("Unparseable command, unable to parse length");
    };

    let length = usize::from_be_bytes(length.try_into().unwrap());
    let log_request_buffer = buf.get(9..(9 + length)).unwrap();
    let log_request: raft::LogRequest = bincode::deserialize(log_request_buffer).unwrap();

    Ok(Command::LogRequest {
        leader_id: log_request.leader_id,
        term: log_request.term,
        prefix_length: log_request.prefix_length,
        prefix_term: log_request.prefix_term,
        leader_commit: log_request.leader_commit,
        suffix: log_request.suffix,
    })
}

#[cfg(test)]
mod tests {
    use crate::command::*;

    #[test]
    fn parse_request_vote_command() {
        let command_byte = 4_u8.to_be_bytes();
        let request = raft::VoteRequest {
            node_id: 1,
            current_term: 2,
            log_length: 10,
            last_term: 1,
        };
        let request_bytes = bincode::serialize(&request).unwrap();
        let length = request_bytes.len().to_be_bytes();
        let mut buf = Vec::new();
        buf.extend(command_byte);
        buf.extend(length);
        buf.extend(request_bytes);

        let command = parse(&buf);

        assert!(command.is_ok());
        assert_eq!(
            command.unwrap(),
            Command::RequestVote {
                node_id: request.node_id,
                current_term: request.current_term,
                log_length: request.log_length,
                last_term: request.last_term
            }
        )
    }

    #[test]
    fn parse_unknown_command() {
        let command_byte = 7_u8.to_be_bytes();
        let command = parse(&command_byte);

        assert!(command.is_ok());
        assert_eq!(command.unwrap(), Command::Unknown)
    }
}
