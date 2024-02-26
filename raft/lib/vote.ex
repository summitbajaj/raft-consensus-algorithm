
# distributed algorithms, n.dulay, 14 jan 2024
# coursework, raft consensus, v2

defmodule Vote do
  # This function is called when an election timeout is received.
  # It handles the transition of a server from follower to candidate.
  def receive_election_timeout(server_state) do
    # Transition to candidate role
    candidate_state = server_state
      |> State.role(:CANDIDATE)               # Change role to candidate
      |> State.inc_term()                     # Increment current term
      |> State.voted_for(server_state.server_num)        # Vote for self
      |> State.add_to_voted_by(server_state.server_num)  # Add self to list of voters
      |> Timer.restart_election_timer()       # Restart election timer

    # Send vote requests to other servers except self
    for server <- candidate_state.servers do
      if server != candidate_state.selfP do
        send server, { :VOTE_REQUEST, State.get_info(candidate_state) }
      end
    end

    candidate_state # return updated server state
  end

  # This function is called when a vote request is received from a candidate.
  # It handles the voting process of a follower.
  def receive_vote_request_from_candidate(follower_state, candidate_term, candidate_num, candidate_pid, candidate_last_log_term, candidate_last_log_index) do
    # Step down if candidate term is greater
    follower_state = if candidate_term > follower_state.curr_term do
      follower_state = stepdown(follower_state, candidate_term)
      follower_state
    else
      follower_state
    end

    # Vote for candidate if conditions are met
    follower_last_log_term = Log.term_at(follower_state, Log.last_index(follower_state))
    follower_state = if (follower_state.voted_for == nil) && ((candidate_last_log_term > follower_last_log_term) || (candidate_last_log_term == follower_last_log_term && candidate_last_log_index >= Log.last_index(follower_state))) do
      follower_state
        |> State.voted_for(candidate_num)     # Vote for candidate
        |> Timer.restart_election_timer()     # Restart election timer
    else
      follower_state                                # If not suited, do not vote and let election timer runout
    end

    # Send vote reply to candidate if voted for them
    if follower_state.voted_for == candidate_num do
      send candidate_pid, {:VOTE_REPLY, follower_state.server_num, follower_state.curr_term}
    end

    follower_state # return updated follower state
  end

  # This function is called when a vote reply is received from a follower.
  # It handles the vote tallying process of a candidate.
  def receive_vote_reply_from_follower(candidate_state, follower_num, follower_term) do
    # Step down if follower's term is larger
    candidate_state = if candidate_state.curr_term < follower_term do
      stepdown(candidate_state, follower_term)
      candidate_state
    else
      candidate_state
    end

    # Add voter into voted_by list if terms match
    candidate_state = if candidate_state.curr_term == follower_term do
      candidate_state |> State.add_to_voted_by(follower_num)
    else
      candidate_state
    end

    # Become leader if majority vote is reached
    candidate_state = if State.vote_tally(candidate_state) >= candidate_state.majority do
      become_leader(candidate_state)
      # # simulate leadership change by crashing leaders
      # Process.exit(self(), :kill)
    else
      candidate_state
    end
  end

  # This function is called to transition a candidate to a leader.
  defp become_leader(candidate_state) do
    # Update candidate's state
    leader_state = candidate_state
      |> State.role(:LEADER)                # Update role to leader
      |> Timer.cancel_election_timer()      # Remove election timer (not needed for leaders)
      |> State.init_next_index()            # Update it's next index with all of its followers to its log length

    # Build the append entries timer for its followers and append to candidate state
    append_entries_timer =
      for server <- leader_state.servers,
      into: Map.new
      do
        if server != leader_state.selfP do
          {server, Timer.leader_create_aeTimer(leader_state, server)}
        else
          {server, nil}
        end
      end

    leader_state = State.add_append_entries_timer(leader_state, append_entries_timer)

    # Announce leadership for this term to followers
    for server <- leader_state.servers do
      if server != leader_state.selfP do
        send server, {:LEADER_ELECTED, leader_state.selfP, leader_state.curr_term}
      end
    end

    leader_state # return updated leader state
  end

  # This function is called when a follower receives a message from a new leader.
  def receive_leader(follower_state, leader_pid, leader_term) do
    # Update follower's term if it's less than leader's term
    follower_state = if follower_state.curr_term < leader_term do
      State.curr_term(follower_state, leader_term)
    else
      follower_state
    end

    follower_state = follower_state
      |> stepdown(leader_term)   # Make sure server steps down in case if was a past leader/candidate
      |> State.leaderP(leader_pid)       # Update the leaderP in State

    follower_state # return updated follower state
  end

  # This function is called to transition a server to a follower when it receives a message from another server of a larger term.
  def stepdown(server_state, term) do
    follower_state = server_state
      |>State.curr_term(term)                     # Update to latest term
      |>State.role(:FOLLOWER)                     # Change role to Follower
      |>State.voted_for(nil)                      # Clear any previous votes
      |>State.new_voted_by()                      # Clear any voted_by  (if any, only for past :LEADERS)
      |>Timer.cancel_all_append_entries_timers()  # Clear aeTimer       (if any, only for past :LEADERS)
      |>Timer.restart_election_timer()            # Restart election timer

    follower_state # return updated follower state
  end
end
