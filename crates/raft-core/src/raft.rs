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

    /// Advances time by one tick.
    ///
    /// A follower or candidate that reaches its election timeout starts a
    /// new election.
    pub fn tick(&mut self) {
        if self.role == Role::Leader {
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
            Message::VoteResponse { .. } => {}
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
    }

    fn become_follower(&mut self, term: Term) {
        self.role = Role::Follower;
        if term > self.term {
            self.term = term;
            self.voted_for = None;
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

    fn config(min: u64, max: u64) -> Config {
        Config {
            id: 1,
            peers: vec![2, 3],
            election_timeout_min: min,
            election_timeout_max: max,
            seed: 42,
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
        let request = Message::RequestVote {
            term: 1,
            candidate: 1,
            last_log_index: 0,
            last_log_term: 0,
        };
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
        let mut raft = Raft::new(config(10, 10));
        tick_n(&mut raft, 10);
        raft.ready();
        assert_eq!(raft.role(), Role::Candidate);

        raft.step(3, request_vote(2, 3, 0, 0));

        assert_eq!(raft.role(), Role::Follower);
        assert_eq!(raft.term(), 2);
        assert_eq!(raft.voted_for(), Some(3));
        assert_eq!(raft.ready().messages, vec![(3, vote_response(2, true))]);
    }

    #[test]
    fn candidate_rejects_a_request_from_its_own_term() {
        let mut raft = Raft::new(config(10, 10));
        tick_n(&mut raft, 10);
        raft.ready();

        // It already voted for itself in term 1.
        raft.step(2, request_vote(1, 2, 0, 0));

        assert_eq!(raft.role(), Role::Candidate);
        assert_eq!(raft.ready().messages, vec![(2, vote_response(1, false))]);
    }

    #[test]
    fn candidate_steps_down_on_a_response_from_a_higher_term() {
        let mut raft = Raft::new(config(10, 10));
        tick_n(&mut raft, 10);
        raft.ready();

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
}
