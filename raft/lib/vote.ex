
# distributed algorithms, n.dulay, 14 jan 2024
# coursework, raft consensus, v2

defmodule Vote do

# when server receives an :ELECTION_TIMEOUT message:
# 1. Node gets promoted to a candidate role
# 2. Increments current term
# 3. Votes for self
# 4. Sends vote requests to all other servers
def handle_election_timeout(s) do
  s = s
  |> State.role(:CANDIDATE)
  |> State.inc_term()
  |> State.voted_for(s.server_num)
  |> Timer.restart_election_timer()

# sending vote requests to other servers
for sever <- s.servers do
  if sever != s.selfP do
    send sever, {:VOTE_REQUEST, State.get_info(s)}
  end
end

s

end

# when sever receives :VOTE_REQUEST message from candidate that have their term more than or equal to follower's term
# 1.
def handle_vote_request_from_candidate(
  follower, # receipient of the vote request
  candidate_curr_term, # term of candidate
  candidate_num, # num of candidate
  candidate_id, # id of candidate
  candidateLastLogTerm, # term of last log entry of candidate
  candidateLastLogIndex # index of last log entry of candidate
  ) do

  # stepdown if candidate's term is greater than follower's term
  follower = if candidate_curr_term > follower.curr_term do
    follower = stepdown(follower, candidate_curr_term)
    
    follower
  else
    follower
  end


  # vote for candidate if
  # 1. Candidate's log is at least as up-to-date as receiver's log
  # 2. OR candidate's log is more up-to-date than receiver's log

  followerLastLogTerm = Log.term_at(follower, Log.last_index(follower))
  should_vote? =
    follower.voted_for == nil and
      (candidateLastLogTerm > followerLastLogTerm or
         (candidateLastLogTerm == followerLastLogTerm and
            candidateLastLogIndex >= Log.last_index(follower)))

  follower =
    if should_vote? do
      follower
      |> State.voted_for(candidate_num) # Vote for candidate
      |> Timer.restart_election_timer() # Restart election timer
    else
      follower # If not suited, do not vote and let election timer runout
    end

  # send vote reply to candidate if voted for them
  if follower.voted_for == candidate_num do
    send candidate_id, {:VOTE_REPLY, follower.server_num, follower.curr_term}
  end
  follower

  end

  # when candidate receives :VOTE_REPLY from other servers:
  def handle_vote_reply_from_follower(
    candidate, # candidate server
    follower_num, # follower's server_num
    follower_curr_term # follower's current term
    ) do

    # Stepdown if follower's term is larger
    candidate = if candidate.curr_term < follower_curr_term do
      stepdown(candidate, follower_curr_term)
      candidate
    else
      candidate
    end

    # If candidates current term is same as followers, add the voter into voted_by
    candidate = if candidate.curr_term == follower_curr_term do
      candidate |> State.add_to_voted_by(follower_num)
    else
      candidate
    end

    # Check if majority reached. If yes, become leader
    candidate = if State.vote_tally(candidate) >= candidate.majority do
      candidate = upgrade_to_leader(candidate)
      candidate
    else
      candidate
    end
  end

  # upgrade a candidate to a leader
  defp upgrade_to_leader(candidate) do

    candidate = candidate
      |> State.role(:LEADER)
      |> Timer.cancel_election_timer()
      |> State.init_next_index()


    # append entries timer for leader to send append entries requests to followers to replicate logs
    aeTimer =
      for i <- candidate.servers,
      into: Map.new
      do
        if i != candidate.selfP do
          {i, Timer.leader_create_aeTimer(candidate, i)}
        else
          {i, nil}
        end
      end

    candidate = State.add_append_entries_timer(candidate, aeTimer)

    # announce leadership for this term to other nodes
    for server <- candidate.servers do
      if server != candidate.selfP do
        send server, {:LEADER_ELECTED, candidate.selfP, candidate.curr_term}
      end
    end

    candidate
  end

  # when followers get a meesage from a new leader
  def handle_new__leader(follower, leaderP, leader_curr_term) do

    # If follower term < leader term, update follower's curr_term to match leader's
    follower = if follower.curr_term < leader_curr_term do
      State.curr_term(follower, leader_curr_term)
    else
      follower
    end

    follower = follower
      |> stepdown(leader_curr_term)
      |> State.leaderP(leaderP)

    follower
  end

  # Used when received message from another server of a larger term.
  def stepdown(server, term) do

    server = server
      |>State.curr_term(term)
      |>State.role(:FOLLOWER)
      |>State.voted_for(nil)
      |>State.new_voted_by()
      |>Timer.cancel_all_append_entries_timers()
      |>Timer.restart_election_timer()

    server
  end


end # Vote
