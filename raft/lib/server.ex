
# distributed algorithms, n.dulay, 14 jan 2024
# coursework, raft consensus, v2

# added messages for leader_elected

defmodule Server do

# _________________________________________________________ Server.start()
def start(config, server_num) do

  config = config
    |> Configuration.node_info("Server", server_num)
    |> Debug.node_starting()

  receive do
  { :BIND, servers, databaseP } ->
    config
    |> State.initialise(server_num, servers, databaseP)
    |> Timer.restart_election_timer()
    |> Server.next()
  end # receive
end # start

# _________________________________________________________ next()
def next(server) do

  # invokes functions in AppendEntries, Vote, ServerLib etc

  server = receive do

  # { :APPEND_ENTRIES_REQUEST, ...

  # { :APPEND_ENTRIES_REPLY, ...

  # { :APPEND_ENTRIES_TIMEOUT, ...

  # handling vote requests

  # _____ Vote Request
  # if candidate's term is lower ->
  { :VOTE_REQUEST, [candidate_curr_term, candidate_num] } when canidate_curr_term < curr_term ->
    s

  # if candidate's term is higher -> vote for candidate
  {:VOTE_REQUEST, [candidate_curr_term, candidate_num, candidate_id, candidateLastLogIndex]} ->
    s = Vote.handle_vote_request_from_candidate(s, candidate_curr_term, candidate_num, candidate_id, candidateLastLogTerm, candidateLastLogIndex)
    s
    # ___

  # ____ Vote Reply
  # if the vote reply is for an old election, it can be discarded
  { :VOTE_REPLY, follower_num, follower_curr_term} when follower_curr_term < curr_term ->
    s
  # else if the server is a candidate, and the vote is for the current election -> accept the vote
  {:VOTE_REPLY, follower_num, follower_curr_term} = msg when s.role = :CANDIDATE ->
    s= Vote.handle_vote_reply_from_follower(s, follower_num, follower_curr_term)

  # if the server is no longer a candidate, ignore the vote
  {:VOTE_REPLY, follower_num} ->
    s
  # ____

# ___ Election Timeout

  # if the election timeout is from an old election -> ignore
  { :ELECTION_TIMEOUT, vote_term} when vote_term < curr_term ->
    s

  # if the election message is sent to the Follower and Candidate -> process it
  { :ELECTION_TIMEOUT} when (s.role != :LEADER) -> Vote.handle_election_timeout(s)

  # else do nothing
  {:ELECION_TIMEOUT} -> s

  ____

# ___ Electing Leaders

  #  if it is a old message about elected leaders -> ignore
  { :LEADER_ELECTED, leader_term } when leader_term < curr_term -> s

  # if the leader election message is from a newer term -> process the leader

  {:LEADER_ELECTED, leaderP, leader_term }  ->
    s = s |> Vote.handle_new_leader(leaderP, leader_term)
    s
  # ___

  # ___ CLient Request

  # if the server is leader, handle client request
  { :CLIENT_REQUEST, m} when s.role == :LEADER ->
    s = s |> ClientRequest.recieve_request_from_client(m)
    s
  # if the server is not a leader -> inform client of leader
  { :CLIENT_REQUEST, m} ->
    send m.clientP, {:CLIENT_REPLY, m.cid, :NOT_LEADER, s.leaderP, s.server_num}
    s

   unexpected ->
      Helper.node_halt("***** Server: unexpected message #{inspect unexpected}")

  end # receive

  server |> Server.next()

end # next

end # Server
