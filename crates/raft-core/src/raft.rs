use std::collections::BTreeSet;

use crate::{Config, HardState, Index, Message, NodeId, RaftLog, Ready, Term};

/// The role a node currently plays.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    /// Waits for a leader. Becomes a candidate if it hears nothing.
    Follower,
    /// Is asking the cluster for votes.
    Candidate,
    /// Has won an election and drives replication.
    Leader,
}

/// The Raft state machine for one node.
///
/// It performs no I/O. Time passes only when [`Raft::tick`] is called, and
/// all output is collected by [`Raft::ready`].
#[derive(Debug)]
pub struct Raft {
    config: Config,
    role: Role,
    term: Term,
    voted_for: Option<NodeId>,
    votes: BTreeSet<NodeId>,
    leader: Option<NodeId>,
    heartbeat_elapsed: u64,
    log: RaftLog,
    ticks_elapsed: u64,
    election_timeout: u64,
    rng: u64,
    ready: Ready,
}

impl Raft {
    /// Creates a follower in term 0 with an empty log.
    #[must_use]
    pub fn new(config: Config) -> Self {
        let rng = config.seed | 1; // xorshift must not start at zero
        let mut raft = Self {
            config,
            role: Role::Follower,
            term: 0,
            voted_for: None,
            votes: BTreeSet::new(),
            leader: None,
            heartbeat_elapsed: 0,
            log: RaftLog::new(),
            ticks_elapsed: 0,
            election_timeout: 0,
            rng,
            ready: Ready::default(),
        };
        raft.reset_election_timer();
        raft
    }

    /// Returns the node's current role.
    #[must_use]
    pub fn role(&self) -> Role {
        self.role
    }

    /// Returns the node's current term.
    #[must_use]
    pub fn term(&self) -> Term {
        self.term
    }

    /// Returns who this node voted for in the current term, if anyone.
    #[must_use]
    pub fn voted_for(&self) -> Option<NodeId> {
        self.voted_for
    }

    /// Returns the leader this node currently recognizes, if any.
    ///
    /// Followers use this to redirect clients.
    #[must_use]
    pub fn leader(&self) -> Option<NodeId> {
        self.leader
    }

    /// Advances time by one tick.
    ///
    /// A leader sends a heartbeat when its interval elapses. A follower or
    /// candidate that reaches its election timeout starts a new election.
    pub fn tick(&mut self) {
        if self.role == Role::Leader {
            self.heartbeat_elapsed += 1;
            if self.heartbeat_elapsed >= self.config.heartbeat_interval {
                self.broadcast_heartbeat();
            }
            return;
        }
        self.ticks_elapsed += 1;
        if self.ticks_elapsed >= self.election_timeout {
            self.start_election();
        }
    }

    /// Handles a message received from node `from`.
    ///
    /// Replies and state changes are collected in [`Raft::ready`].
    pub fn step(&mut self, from: NodeId, message: Message) {
        // rules for all servers: a higher term always wins.
        if message.term() > self.term {
            self.become_follower(message.term());
        }

        match message {
            Message::RequestVote {
                term,
                candidate,
                last_log_index,
                last_log_term,
            } => {
                self.handle_request_vote(from, term, candidate, last_log_index, last_log_term);
            }
            Message::VoteResponse { term, granted } => {
                self.handle_vote_response(from, term, granted);
            }
            Message::AppendEntries { term, leader } => {
                self.handle_append_entries(from, term, leader);
            }
            // Only the term check above matters until replication exists.
            Message::AppendResponse { .. } => {}
        }
    }

    /// Takes everything the runtime must persist and send.
    pub fn ready(&mut self) -> Ready {
        std::mem::take(&mut self.ready)
    }

    fn start_election(&mut self) {
        self.role = Role::Candidate;
        self.term += 1;
        self.voted_for = Some(self.config.id);
        self.votes.clear();
        self.votes.insert(self.config.id);
        self.leader = None;
        self.reset_election_timer();
        self.persist_hard_state();

        let request = Message::RequestVote {
            term: self.term,
            candidate: self.config.id,
            last_log_index: self.log.last_index(),
            last_log_term: self.log.last_term(),
        };
        for &peer in &self.config.peers {
            self.ready.messages.push((peer, request.clone()));
        }

        // A single-node cluster already has its majority.
        if self.votes.len() >= self.quorum() {
            self.become_leader();
        }
    }

    fn become_follower(&mut self, term: Term) {
        self.role = Role::Follower;
        self.votes.clear();
        if term > self.term {
            self.term = term;
            self.voted_for = None;
            self.leader = None;
            self.persist_hard_state();
        }
        self.reset_election_timer();
    }

    fn handle_request_vote(
        &mut self,
        from: NodeId,
        term: Term,
        candidate: NodeId,
        last_log_index: Index,
        last_log_term: Term,
    ) {
        // Comparing (term, index) pairs checks the term first, then the length.
        let log_is_current =
            (last_log_term, last_log_index) >= (self.log.last_term(), self.log.last_index());
        let can_vote = self.voted_for.is_none() || self.voted_for == Some(candidate);
        // A lower term is rejected. A higher term was already adopted in `step`.
        let granted = term == self.term && can_vote && log_is_current;

        if granted {
            self.voted_for = Some(candidate);
            self.persist_hard_state();
            self.reset_election_timer();
        }
        self.ready.messages.push((
            from,
            Message::VoteResponse {
                term: self.term,
                granted,
            },
        ));
    }

    fn handle_vote_response(&mut self, from: NodeId, term: Term, granted: bool) {
        // Only count grants for the election we are running right now.
        // A higher term already turned us into a follower in `step`, and a
        // lower term is a late reply from an earlier election.
        if self.role != Role::Candidate || term != self.term || !granted {
            return;
        }
        self.votes.insert(from);
        if self.votes.len() >= self.quorum() {
            self.become_leader();
        }
    }

    fn become_leader(&mut self) {
        self.role = Role::Leader;
        self.leader = Some(self.config.id);
        // Figure 2: on election, send an initial heartbeat to every server.
        self.broadcast_heartbeat();
    }

    fn broadcast_heartbeat(&mut self) {
        self.heartbeat_elapsed = 0;
        let heartbeat = Message::AppendEntries {
            term: self.term,
            leader: self.config.id,
        };
        for &peer in &self.config.peers {
            self.ready.messages.push((peer, heartbeat.clone()));
        }
    }

    fn handle_append_entries(&mut self, from: NodeId, term: Term, leader: NodeId) {
        // Rule 1: a leader from an older term is stale. Tell it our term.
        if term < self.term {
            self.ready.messages.push((
                from,
                Message::AppendResponse {
                    term: self.term,
                    success: false,
                },
            ));
            return;
        }
        // Election safety makes two leaders in one term impossible, so a
        // leader never gets here with an equal term. A higher term already
        // made us a follower in `step`.
        if self.role == Role::Leader {
            return;
        }
        // A current leader exists: a candidate gives up, and a follower
        // restarts its election timer.
        self.become_follower(term);
        self.leader = Some(leader);
        self.ready.messages.push((
            from,
            Message::AppendResponse {
                term: self.term,
                success: true,
            },
        ));
    }

    /// Returns how many nodes make a majority of the cluster.
    ///
    /// `peers` excludes this node, so the cluster has `peers.len() + 1`
    /// nodes and its majority is half of that, rounded up.
    fn quorum(&self) -> usize {
        self.config.peers.len().div_ceil(2) + 1
    }

    /// Records that term or vote changed and must be saved before sending.
    fn persist_hard_state(&mut self) {
        self.ready.hard_state = Some(HardState {
            current_term: self.term,
            voted_for: self.voted_for,
        });
    }

    /// Picks a new random election timeout and restarts the countdown.
    fn reset_election_timer(&mut self) {
        let min = self.config.election_timeout_min;
        let max = self.config.election_timeout_max.max(min);
        let span = (max - min).saturating_add(1);
        self.election_timeout = min + self.next_random() % span;
        self.ticks_elapsed = 0;
    }

    /// A small seeded xorshift generator. Deterministic by design.
    fn next_random(&mut self) -> u64 {
        let mut x = self.rng;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.rng = x;
        x
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Node 1 in a three-node cluster, with a heartbeat every 3 ticks.
    fn config(min: u64, max: u64) -> Config {
        Config {
            id: 1,
            peers: vec![2, 3],
            election_timeout_min: min,
            election_timeout_max: max,
            heartbeat_interval: 3,
            seed: 42,
        }
    }

    fn five_node_config() -> Config {
        Config {
            peers: vec![2, 3, 4, 5],
            ..config(10, 10)
        }
    }

    fn single_node_config() -> Config {
        Config {
            peers: vec![],
            ..config(10, 10)
        }
    }

    fn tick_n(raft: &mut Raft, n: u64) {
        for _ in 0..n {
            raft.tick();
        }
    }

    fn request_vote(
        term: Term,
        candidate: NodeId,
        last_log_index: Index,
        last_log_term: Term,
    ) -> Message {
        Message::RequestVote {
            term,
            candidate,
            last_log_index,
            last_log_term,
        }
    }

    fn vote_response(term: Term, granted: bool) -> Message {
        Message::VoteResponse { term, granted }
    }

    fn append_entries(term: Term, leader: NodeId) -> Message {
        Message::AppendEntries { term, leader }
    }

    fn append_response(term: Term, success: bool) -> Message {
        Message::AppendResponse { term, success }
    }

    /// Returns a node that has just started an election in term 1,
    /// with its `Ready` drained.
    fn candidate(config: Config) -> Raft {
        let mut raft = Raft::new(config);
        tick_n(&mut raft, 10);
        raft.ready();
        raft
    }

    /// Returns node 1 elected leader of term 1, with its `Ready` drained.
    fn leader_of(config: Config) -> Raft {
        let mut raft = candidate(config);
        raft.step(2, vote_response(1, true));
        raft.ready();
        raft
    }

    /// Ticks until the node becomes a candidate and returns how many it took.
    fn ticks_until_candidate(config: Config) -> u64 {
        let mut raft = Raft::new(config);
        for tick in 1..=1000 {
            raft.tick();
            if raft.role() == Role::Candidate {
                return tick;
            }
        }
        panic!("never became a candidate");
    }

    // Election timeout

    #[test]
    fn new_node_is_a_follower_in_term_zero() {
        let raft = Raft::new(config(10, 20));
        assert_eq!(raft.role(), Role::Follower);
        assert_eq!(raft.term(), 0);
        assert_eq!(raft.voted_for(), None);
    }

    #[test]
    fn stays_follower_before_the_timeout() {
        let mut raft = Raft::new(config(10, 10));
        tick_n(&mut raft, 9);
        assert_eq!(raft.role(), Role::Follower);
        assert_eq!(raft.term(), 0);
    }

    #[test]
    fn becomes_candidate_at_the_timeout() {
        let mut raft = Raft::new(config(10, 10));
        tick_n(&mut raft, 10);
        assert_eq!(raft.role(), Role::Candidate);
        assert_eq!(raft.term(), 1);
        assert_eq!(raft.voted_for(), Some(1));
    }

    #[test]
    fn election_asks_every_peer_for_a_vote_and_requests_persistence() {
        let mut raft = Raft::new(config(10, 10));
        tick_n(&mut raft, 10);
        let ready = raft.ready();

        assert_eq!(
            ready.hard_state,
            Some(HardState {
                current_term: 1,
                voted_for: Some(1)
            })
        );
        let request = request_vote(1, 1, 0, 0);
        assert_eq!(ready.messages, vec![(2, request.clone()), (3, request)]);
    }

    #[test]
    fn ready_drains_its_contents() {
        let mut raft = Raft::new(config(10, 10));
        tick_n(&mut raft, 10);
        raft.ready();
        assert_eq!(raft.ready(), Ready::default());
    }

    #[test]
    fn candidate_that_times_out_again_starts_a_new_election() {
        let mut raft = Raft::new(config(10, 10));
        tick_n(&mut raft, 10);
        assert_eq!(raft.term(), 1);

        // The timer was reset, so 9 more ticks change nothing.
        tick_n(&mut raft, 9);
        assert_eq!(raft.term(), 1);

        raft.tick();
        assert_eq!(raft.term(), 2);
        assert_eq!(raft.role(), Role::Candidate);
    }

    #[test]
    fn same_seed_gives_same_timeout_and_it_stays_in_range() {
        let first = ticks_until_candidate(config(10, 20));
        let second = ticks_until_candidate(config(10, 20));
        assert_eq!(first, second);
        assert!((10..=20).contains(&first));
    }

    // Handling RequestVote

    #[test]
    fn grants_first_vote_and_persists_it() {
        let mut raft = Raft::new(config(10, 10));
        raft.step(2, request_vote(1, 2, 0, 0));
        let ready = raft.ready();

        assert_eq!(raft.role(), Role::Follower);
        assert_eq!(raft.term(), 1);
        assert_eq!(raft.voted_for(), Some(2));
        assert_eq!(ready.messages, vec![(2, vote_response(1, true))]);
        assert_eq!(
            ready.hard_state,
            Some(HardState {
                current_term: 1,
                voted_for: Some(2)
            })
        );
    }

    #[test]
    fn rejects_a_request_from_a_lower_term() {
        let mut raft = Raft::new(config(10, 10));
        raft.term = 5;
        raft.step(2, request_vote(3, 2, 0, 0));
        let ready = raft.ready();

        assert_eq!(raft.term(), 5);
        assert_eq!(ready.messages, vec![(2, vote_response(5, false))]);
        assert_eq!(ready.hard_state, None);
    }

    #[test]
    fn votes_for_only_one_candidate_per_term() {
        let mut raft = Raft::new(config(10, 10));
        raft.step(2, request_vote(1, 2, 0, 0));
        raft.step(3, request_vote(1, 3, 0, 0));
        let ready = raft.ready();

        assert_eq!(raft.voted_for(), Some(2));
        assert_eq!(
            ready.messages,
            vec![(2, vote_response(1, true)), (3, vote_response(1, false))]
        );
    }

    #[test]
    fn repeated_request_from_the_same_candidate_is_granted_again() {
        let mut raft = Raft::new(config(10, 10));
        raft.step(2, request_vote(1, 2, 0, 0));
        raft.step(2, request_vote(1, 2, 0, 0));
        let ready = raft.ready();

        assert_eq!(
            ready.messages,
            vec![(2, vote_response(1, true)), (2, vote_response(1, true))]
        );
    }

    #[test]
    fn rejects_a_candidate_with_an_older_last_term_even_if_its_log_is_longer() {
        let mut raft = Raft::new(config(10, 10));
        raft.log.push(2, Vec::new());
        raft.step(2, request_vote(1, 2, 5, 1));
        let ready = raft.ready();

        assert_eq!(ready.messages, vec![(2, vote_response(1, false))]);
        // The higher term is still adopted, and the vote stays free.
        assert_eq!(raft.term(), 1);
        assert_eq!(raft.voted_for(), None);
        assert_eq!(
            ready.hard_state,
            Some(HardState {
                current_term: 1,
                voted_for: None
            })
        );
    }

    #[test]
    fn rejects_a_candidate_with_the_same_last_term_but_a_shorter_log() {
        let mut raft = Raft::new(config(10, 10));
        raft.log.push(2, Vec::new());
        raft.log.push(2, Vec::new());
        raft.step(2, request_vote(1, 2, 1, 2));

        assert_eq!(raft.ready().messages, vec![(2, vote_response(1, false))]);
    }

    #[test]
    fn grants_a_candidate_whose_log_is_equally_up_to_date() {
        let mut raft = Raft::new(config(10, 10));
        raft.log.push(2, Vec::new());
        raft.log.push(2, Vec::new());
        raft.step(2, request_vote(1, 2, 2, 2));

        assert_eq!(raft.ready().messages, vec![(2, vote_response(1, true))]);
    }

    #[test]
    fn grants_a_candidate_with_a_newer_last_term_even_if_its_log_is_shorter() {
        let mut raft = Raft::new(config(10, 10));
        raft.log.push(2, Vec::new());
        raft.log.push(2, Vec::new());
        raft.step(2, request_vote(1, 2, 1, 3));

        assert_eq!(raft.ready().messages, vec![(2, vote_response(1, true))]);
    }

    #[test]
    fn candidate_steps_down_on_a_request_from_a_higher_term() {
        let mut raft = candidate(config(10, 10));
        assert_eq!(raft.role(), Role::Candidate);

        raft.step(3, request_vote(2, 3, 0, 0));

        assert_eq!(raft.role(), Role::Follower);
        assert_eq!(raft.term(), 2);
        assert_eq!(raft.voted_for(), Some(3));
        assert_eq!(raft.ready().messages, vec![(3, vote_response(2, true))]);
    }

    #[test]
    fn candidate_rejects_a_request_from_its_own_term() {
        let mut raft = candidate(config(10, 10));

        // It already voted for itself in term 1.
        raft.step(2, request_vote(1, 2, 0, 0));

        assert_eq!(raft.role(), Role::Candidate);
        assert_eq!(raft.ready().messages, vec![(2, vote_response(1, false))]);
    }

    #[test]
    fn candidate_steps_down_on_a_response_from_a_higher_term() {
        let mut raft = candidate(config(10, 10));

        raft.step(2, vote_response(2, false));

        assert_eq!(raft.role(), Role::Follower);
        assert_eq!(raft.term(), 2);
        assert_eq!(raft.voted_for(), None);
    }

    #[test]
    fn granting_a_vote_restarts_the_election_timer() {
        let mut raft = Raft::new(config(10, 10));
        tick_n(&mut raft, 9);
        raft.step(2, request_vote(1, 2, 0, 0));

        // The countdown restarted, so 9 more ticks are not enough.
        tick_n(&mut raft, 9);
        assert_eq!(raft.role(), Role::Follower);
        assert_eq!(raft.term(), 1);

        raft.tick();
        assert_eq!(raft.role(), Role::Candidate);
        assert_eq!(raft.term(), 2);
    }

    // Counting votes and winning

    #[test]
    fn candidate_becomes_leader_with_a_majority() {
        let mut raft = candidate(config(10, 10));
        raft.step(2, vote_response(1, true));
        assert_eq!(raft.role(), Role::Leader);
        assert_eq!(raft.term(), 1);
    }

    #[test]
    fn five_node_cluster_needs_three_votes() {
        let mut raft = candidate(five_node_config());
        raft.step(2, vote_response(1, true));
        assert_eq!(raft.role(), Role::Candidate);
        raft.step(3, vote_response(1, true));
        assert_eq!(raft.role(), Role::Leader);
    }

    #[test]
    fn rejected_votes_do_not_count() {
        let mut raft = candidate(config(10, 10));
        raft.step(2, vote_response(1, false));
        raft.step(3, vote_response(1, false));
        assert_eq!(raft.role(), Role::Candidate);
    }

    #[test]
    fn duplicate_grant_from_one_peer_counts_once() {
        let mut raft = candidate(five_node_config());
        raft.step(2, vote_response(1, true));
        raft.step(2, vote_response(1, true));
        assert_eq!(raft.role(), Role::Candidate);
    }

    #[test]
    fn grant_for_an_earlier_election_is_ignored() {
        let mut raft = candidate(config(10, 10));
        tick_n(&mut raft, 10); // times out again, now in term 2
        assert_eq!(raft.term(), 2);

        raft.step(2, vote_response(1, true));
        assert_eq!(raft.role(), Role::Candidate);
    }

    #[test]
    fn votes_are_cleared_for_each_new_election() {
        let mut raft = candidate(five_node_config());
        raft.step(2, vote_response(1, true));
        tick_n(&mut raft, 10); // new election, term 2
        assert_eq!(raft.term(), 2);

        raft.step(3, vote_response(2, true));
        // Only nodes 1 and 3 voted in term 2. Node 2's old vote must not count.
        assert_eq!(raft.role(), Role::Candidate);
    }

    #[test]
    fn leader_does_not_start_elections() {
        let mut raft = leader_of(config(10, 10));
        tick_n(&mut raft, 100);
        assert_eq!(raft.role(), Role::Leader);
        assert_eq!(raft.term(), 1);
    }

    #[test]
    fn extra_grants_after_winning_change_nothing() {
        let mut raft = candidate(config(10, 10));
        raft.step(2, vote_response(1, true));
        raft.step(3, vote_response(1, true));
        assert_eq!(raft.role(), Role::Leader);
        assert_eq!(raft.term(), 1);
    }

    #[test]
    fn single_node_cluster_wins_immediately() {
        let mut raft = Raft::new(single_node_config());
        tick_n(&mut raft, 10);

        assert_eq!(raft.role(), Role::Leader);
        assert_eq!(raft.term(), 1);
        let ready = raft.ready();
        assert!(ready.messages.is_empty());
        assert_eq!(
            ready.hard_state,
            Some(HardState {
                current_term: 1,
                voted_for: Some(1)
            })
        );
    }

    #[test]
    fn leader_steps_down_on_a_request_from_a_higher_term() {
        let mut raft = leader_of(config(10, 10));

        raft.step(3, request_vote(2, 3, 0, 0));

        assert_eq!(raft.role(), Role::Follower);
        assert_eq!(raft.term(), 2);
        assert_eq!(raft.ready().messages, vec![(3, vote_response(2, true))]);
    }

    #[test]
    fn leader_rejects_a_vote_request_in_its_own_term() {
        let mut raft = leader_of(config(10, 10));

        raft.step(3, request_vote(1, 3, 0, 0));

        assert_eq!(raft.role(), Role::Leader);
        assert_eq!(raft.ready().messages, vec![(3, vote_response(1, false))]);
    }

    #[test]
    fn follower_ignores_vote_responses() {
        let mut raft = Raft::new(config(10, 10));
        raft.step(2, vote_response(0, true));
        assert_eq!(raft.role(), Role::Follower);
    }

    // Heartbeats

    #[test]
    fn new_leader_sends_a_heartbeat_to_every_peer_immediately() {
        let mut raft = candidate(config(10, 10));
        raft.step(2, vote_response(1, true));

        assert_eq!(raft.leader(), Some(1));
        assert_eq!(
            raft.ready().messages,
            vec![(2, append_entries(1, 1)), (3, append_entries(1, 1))]
        );
    }

    #[test]
    fn leader_sends_heartbeats_every_interval() {
        let mut raft = leader_of(config(10, 10));

        tick_n(&mut raft, 2);
        assert!(raft.ready().messages.is_empty());
        raft.tick();
        assert_eq!(
            raft.ready().messages,
            vec![(2, append_entries(1, 1)), (3, append_entries(1, 1))]
        );

        tick_n(&mut raft, 2);
        assert!(raft.ready().messages.is_empty());
        raft.tick();
        assert_eq!(raft.ready().messages.len(), 2);
    }

    #[test]
    fn follower_accepts_a_heartbeat_learns_the_leader_and_restarts_its_timer() {
        let mut raft = Raft::new(config(10, 10));
        tick_n(&mut raft, 9);
        raft.step(2, append_entries(1, 2));
        let ready = raft.ready();

        assert_eq!(raft.role(), Role::Follower);
        assert_eq!(raft.term(), 1);
        assert_eq!(raft.leader(), Some(2));
        assert_eq!(ready.messages, vec![(2, append_response(1, true))]);
        assert_eq!(
            ready.hard_state,
            Some(HardState {
                current_term: 1,
                voted_for: None
            })
        );

        // The timer restarted, so 9 more ticks are not enough.
        tick_n(&mut raft, 9);
        assert_eq!(raft.role(), Role::Follower);
        raft.tick();
        assert_eq!(raft.role(), Role::Candidate);
        assert_eq!(raft.term(), 2);
    }

    #[test]
    fn regular_heartbeats_keep_a_follower_from_starting_an_election() {
        let mut raft = Raft::new(config(10, 10));
        for _ in 0..100 {
            tick_n(&mut raft, 5);
            raft.step(2, append_entries(1, 2));
        }
        assert_eq!(raft.role(), Role::Follower);
        assert_eq!(raft.term(), 1);
    }

    #[test]
    fn follower_rejects_a_heartbeat_from_a_lower_term_without_restarting_its_timer() {
        let mut raft = Raft::new(config(10, 10));
        raft.term = 5;
        tick_n(&mut raft, 9);
        raft.step(2, append_entries(3, 2));
        let ready = raft.ready();

        assert_eq!(ready.messages, vec![(2, append_response(5, false))]);
        assert_eq!(ready.hard_state, None);
        assert_eq!(raft.leader(), None);

        // The stale heartbeat must not have reset the timer.
        raft.tick();
        assert_eq!(raft.role(), Role::Candidate);
        assert_eq!(raft.term(), 6);
    }

    #[test]
    fn candidate_steps_down_for_a_leader_of_its_own_term() {
        let mut raft = candidate(config(10, 10));
        raft.step(2, append_entries(1, 2));

        assert_eq!(raft.role(), Role::Follower);
        assert_eq!(raft.term(), 1);
        assert_eq!(raft.voted_for(), Some(1));
        assert_eq!(raft.leader(), Some(2));
        assert_eq!(raft.ready().messages, vec![(2, append_response(1, true))]);
    }

    #[test]
    fn candidate_ignores_a_heartbeat_from_a_stale_leader() {
        let mut raft = candidate(config(10, 10));
        raft.step(2, append_entries(0, 2));

        assert_eq!(raft.role(), Role::Candidate);
        assert_eq!(raft.ready().messages, vec![(2, append_response(1, false))]);
    }

    #[test]
    fn leader_steps_down_for_a_heartbeat_from_a_higher_term() {
        let mut raft = leader_of(config(10, 10));
        raft.step(3, append_entries(2, 3));

        assert_eq!(raft.role(), Role::Follower);
        assert_eq!(raft.term(), 2);
        assert_eq!(raft.leader(), Some(3));
        assert_eq!(raft.ready().messages, vec![(3, append_response(2, true))]);

        // It no longer sends heartbeats of its own.
        tick_n(&mut raft, 3);
        assert!(raft.ready().messages.is_empty());
    }

    #[test]
    fn leader_ignores_a_heartbeat_in_its_own_term() {
        // Impossible in a correct cluster; the leader must not step down.
        let mut raft = leader_of(config(10, 10));
        raft.step(3, append_entries(1, 3));

        assert_eq!(raft.role(), Role::Leader);
        assert!(raft.ready().messages.is_empty());
    }

    #[test]
    fn leader_steps_down_on_a_response_from_a_higher_term() {
        let mut raft = leader_of(config(10, 10));
        raft.step(2, append_response(2, false));

        assert_eq!(raft.role(), Role::Follower);
        assert_eq!(raft.term(), 2);
    }
}
