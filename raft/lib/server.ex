
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


  # ___Append Entries Request

  # sending a heartbeat request
  { :APPEND_ENTRIES_REQUEST} ->
    server = Timer.restart_election_timer(server)

  # new append request
  { :APPEND_ENTRIES_REQUEST, leaderTerm, prevIndex, prevTerm, leaderEntries, commitIndex} ->
    server = server
      |> Timer.restart_election_timer()
      |> AppendEntries.receive_append_entries_request(leaderTerm, prevIndex, prevTerm, leaderEntries, commitIndex)

  # _____

  # ___ Append Entries Reply

  # response to append entries request from leader
  { :APPEND_ENTRIES_REPLY, followerP, followerTerm, success, followerLastIndex} when server.role == :Leader ->
    # if the follower'server term is higher, step down
    server = if followerTerm > server.curr_term do
      server = Vote.stepdown(server, followerTerm)
    else
      server = AppendEntries.receive_append_entries_reply_from_follower(server, followerP, followerTerm, success, followerLastIndex)
    end
    server

  # if the server is not a leader ignore
  {:APPEND_ENTRIES_REPLY} -> server

  # _____ Append Entries Timeout

  # when append entries timer runs out -> resend the request
  { :APPEND_ENTRIES_TIMEOUT, term, followerP} ->
    # if server is still a leader, resend the request
    server = if server.role == :LEADER && term == server.curr_term do
      server = server
      |> AppendEntries.send_entries_to_follower(followerP)

      # if not leader then just ignore
    else
      server
    end
    server
  # ___

  # handling vote requests

  # _____ Vote Request
  # if candidate'server term is lower ->
  { :VOTE_REQUEST, [candidate_curr_term, candidate_num] } when candidate_curr_term < server.curr_term ->
    server

  # if candidate'server term is higher -> vote for candidate
  {:VOTE_REQUEST, [candidate_curr_term, candidate_num, candidate_id, candidateLastLogTerm ,candidateLastLogIndex]} ->
    server = Vote.handle_vote_request_from_candidate(server, candidate_curr_term, candidate_num, candidate_id, candidateLastLogTerm, candidateLastLogIndex)
    server
    # ___

  # ____ Vote Reply
  # if the vote reply is for an old election, it can be discarded
  { :VOTE_REPLY, follower_num, follower_curr_term} when follower_curr_term < server.curr_term -> server
  # else if the server is a candidate, and the vote is for the current election -> accept the vote
  {:VOTE_REPLY, follower_num, follower_curr_term} = msg when server.role == :CANDIDATE ->
    server= Vote.handle_vote_reply_from_follower(server, follower_num, follower_curr_term)

  # if the server is no longer a candidate, ignore the vote
  {:VOTE_REPLY, follower_num} -> server
  # ____

# ___ Election Timeout

  # if the election timeout is from an old election -> ignore
  { :ELECTION_TIMEOUT, vote_term} when vote_term < server.curr_term ->
    server

  # if the election message is sent to the Follower and Candidate -> process it
  { :ELECTION_TIMEOUT} when (server.role != :LEADER) ->
    server = Vote.handle_election_timeout(server)

  # else do nothing
  {:ELECION_TIMEOUT} -> server

  # ____

# ___ Electing Leaders

  #  if it is a old message about elected leaders -> ignore
  { :LEADER_ELECTED, leader_term } when leader_term < server.curr_term -> server

  # if the leader election message is from a newer term -> process the leader

  {:LEADER_ELECTED, leaderP, leader_term }  ->
    server = server |> Vote.handle_new_leader(leaderP, leader_term)
    server
  # ___

  # ___ CLient Request

  # if the server is leader, handle client request
  { :CLIENT_REQUEST, m} when server.role == :LEADER ->
    server = server |> ClientRequest.process_request_from_client(m)
    server
  # if the server is not a leader -> inform client of leader
  { :CLIENT_REQUEST, m} ->
    send m.clientP, {:CLIENT_REPLY, m.cid, :NOT_LEADER, server.leaderP, server.server_num}
    server

  # ____ COMMIT ENTRIES REQUEST
  # replicate log to server's database
  {:COMMIT_ENTRIES_REQUEST, db_seqnum} when db_seqnum > server.last_applied ->
    for index <- (server.last_applied+1)..min(db_seqnum, server.commit_index) do
      send server.databaseP, { :DB_REQUEST, Log.request_at(server, index), index}
    end
    server

  # if it has been replicated -> ignore
  {:COMMIT_ENTRIES_REQUEST, db_seqnum} -> server
  # ____

  # ____ DB Messages

  # If db replication was successful for leader
  { :DB_REPLY, db_seqnum, client_request } when server.role == :LEADER ->
    server = ClientRequest.process_reply_from_db(server, db_seqnum, client_request)
    server

  # If db replication was successful for follower/ candidate
  { :DB_REPLY, db_seqnum, client_request } when server.last_applied < db_seqnum ->
    server = State.last_applied(server, db_seqnum)
    server

  # Otherwise, ignore
  { :DB_REPLY, client_request } -> server

   unexpected ->
      Helper.node_halt("***** Server: unexpected message #{inspect unexpected}")

  end # receive

  server |> Server.next()

end # next

end # Server
