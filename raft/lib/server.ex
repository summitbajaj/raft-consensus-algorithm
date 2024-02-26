
# distributed algorithms, n.dulay, 14 jan 2024
# coursework, raft consensus, v2

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

    server = receive do
    # ------------------------------------------------------- #
    # ---------------- VOTE-RELATED MESSAGES ---------------- #

    # ________________ I. ELECTION TIMEOUT __________________ #
    # Origin: Self (Follower/Candidate)  >> Destination: Self (Follower/Candidate)
    # Description: Triggered when the election timer expires
    # Message variables: election_term - the server's current term during which the election timeout occurred

    # If the election timeout message was from an old term, ignore it:
    { :ELECTION_TIMEOUT, election_term, _election } when election_term < server.curr_term ->
      # No action is taken if the election timeout is from an old term
      server

    # If the election timeout message is from the current term and the server is not a Leader, process it:
    { :ELECTION_TIMEOUT, _election_term, _election } when (server.role != :LEADER) ->
      # The server processes the election timeout and potentially starts a new election round
      server = Vote.receive_election_timeout(server)

    { :ELECTION_TIMEOUT, _election_term, _election } ->
      # If the server is a Leader, the election timeout message is ignored
      server

    # _________________ II. VOTE REQUEST ____________________ #
    # Origin: Candidate >> Destination: Follower
    # Description: Triggered when a Candidate requests a vote from a Follower
    # Message variables:
    # candidate_term - the current term of the Candidate
    # candidate_number - the unique identifier of the Candidate
    # candidate_id - the process id of the Candidate
    # candidate_last_log_term - the term of the Candidate's last log entry
    # candidate_last_log_index - the index of the Candidate's last log entry

    # If the Candidate's term is lower than the server's current term, ignore the vote request:
    { :VOTE_REQUEST, [candidate_term, candidate_number, _candidate_id, _candidate_last_log_term, _candidate_last_log_index] } when candidate_term < server.curr_term ->
      # The server does not vote for a Candidate from a previous term
      server

    # If the Candidate's term is not lower than the server's current term, consider the vote request:
    { :VOTE_REQUEST, [candidate_term, candidate_number, candidate_id, candidate_last_log_term, candidate_last_log_index] } ->
      # The server processes the vote request from the Candidate
      server = Vote.receive_vote_request_from_candidate(server, candidate_term, candidate_number, candidate_id, candidate_last_log_term, candidate_last_log_index)


    # ____________________ III. VOTE REPLY ____________________ #
    # From: Follower >> To: Candidate
    # Description: when received a vote reply from followers.
    # Message variables:
    #   - follower_num       : follower'server server_num
    #   - follower_curr_term : follower'server current term

    # If the vote reply was for an old election, discard:
    { :VOTE_REPLY, follower_num, follower_curr_term } when follower_curr_term < server.curr_term ->
      # IO.puts("Old Vote Reply from Server #{follower_num} to Server #{server.server_num}. Ignored.")
      server

    # Else if, the server is still a candidate and vote reply is for this term'server election, accept:
    { :VOTE_REPLY, follower_num, follower_curr_term } = msg when server.role == :CANDIDATE ->
      server = Vote.receive_vote_reply_from_follower(server, follower_num, follower_curr_term)

    # Else, server is no longer a Candidate (either Follower/Leader), ignore:
    { :VOTE_REPLY, follower_num, _follower_curr_term } ->
      # IO.puts("Server #{follower_num} is currently a #{server.role}. Ignored vote reply.")
      server

      # __________________ IV. LEADER ELECTED ____________________ #
      # From: New Leader >> To: Candidate/ Follower
      # Description: received when a new leader has been elected.
      # Message variables:
      #   - leaderP          : leader'server <PID>
      #   - leader_curr_term : leader'server current term

      # If it is a old leader elected message, ignore.
      {:LEADER_ELECTED, _leaderP, leader_curr_term} when leader_curr_term < server.curr_term ->
        # IO.puts("Old leader message (From term #{leader_curr_term}, Now term #{server.curr_term}). Discard message.")
        server

      # Otherwise, process new leader.
      {:LEADER_ELECTED, leaderP, leader_curr_term} ->
        server = server |> Vote.receive_leader(leaderP, leader_curr_term)
        server


    # ------------------------------------------------------- #
    # ---------------- APPEND-ENTRIES MESSAGES -------------- #
    # ------------------------------------------------------- #

    # ______________ I. APPEND-ENTRIES REQUEST ______________ #
    # From: Leader >> To: Followers
    # Description: received aeRequest from leader
    # Message Variables:
    #   - leaderTerm    : leader'server server.curr_term
    #   - commitIndex   : leader'server commitIndex
    #   - prevIndex     : the index where leader and follower'server logs coincide
    #   - prevTerm      : the log'server term at prevIndex
    #   - leaderEntries : the leader'server logs from [prevIndex + 1 .. end]

    # Heartbeat
    { :APPEND_ENTRIES_REQUEST, _leaderTerm, _commitIndex } ->
      # IO.puts("Server #{server.server_num} received heartbeat, restarting timer - Line 121 server.ex")
      server = Timer.restart_election_timer(server)

    # New Append Entries Request
    { :APPEND_ENTRIES_REQUEST, leaderTerm, prevIndex, prevTerm, leaderEntries, commitIndex} ->
      # IO.puts("Server #{server.server_num} received aeReq from leader, processing - Line 126 server.ex")
      server = server
        |> Timer.restart_election_timer()
        |> AppendEntries.receive_append_entries_request(leaderTerm, prevIndex, prevTerm, leaderEntries, commitIndex)


    # ______________ II. APPEND-ENTRIES REPLY ______________ #
    # From: Follower >> To: Leader
    # Description: response to the aeRequest sent by leader previously. Could either be a success/ failure.

    # If server is indeed a leader,
    {:APPEND_ENTRIES_REPLY, followerP, followerTerm, success, followerLastIndex} when server.role == :LEADER ->
      # If received reply from a follower with a larger term, stepdown:
      server = if followerTerm > server.curr_term do
        # IO.puts("Leader term #{server.server.curr_term} smaller than follower term #{followerTerm}, stepdown")
        server = Vote.stepdown(server, followerTerm)
      # Otherwise, process reply
      else
        server = AppendEntries.receive_append_entries_reply_from_follower(server, followerP, followerTerm, success, followerLastIndex)
      end
      server

    # If server is not a leader, ignore:
    {:APPEND_ENTRIES_REPLY, _followerP, _followerTerm, _success, _followerLastIndex} ->
      # IO.puts("Server #{server.server_num} ignored aeReply as no longer leader")
      server

    # ______________ III. APPEND-ENTRIES TIMEOUT ______________ #
    # From: Leader >> To: Leader
    # Description: when append-entries timer for a follower runsout, resend aeRequests/ heartbeats to follower.

    { :APPEND_ENTRIES_TIMEOUT, term, followerP } ->
      # If server is still a leader and the timeout is for the current term, process it
      server = if server.role == :LEADER && term == server.curr_term do
        server = server
        |> AppendEntries.send_entries_to_followers(followerP)

      # Otherwise, this is an outdated message, ignore.
      else
        # IO.puts("Server received outdated aeTimeout (either not leader/ from older term")
        server
      end

      server

    # ______________ IV. COMMIT_ENTRIES_REQUEST ______________ #
    # From: Leader >> To: Followers
    # Description: Sent after a leader has replicated its logs to its database.
    #              Followers instructed to replicate logs to database.

    # If the log has not been replicated to the server'server database, replicate
    {:COMMIT_ENTRIES_REQUEST, db_seqnum} when db_seqnum > server.last_applied ->
      for index <- (server.last_applied+1)..min(db_seqnum, server.commit_index) do
        # IO.puts("Follower committing log #{index} to database")
        send server.databaseP, { :DB_REQUEST, Log.request_at(server, index), index}
      end
      server

    # If already replicated, ignore the request
    {:COMMIT_ENTRIES_REQUEST, db_seqnum} ->
      # IO.puts("Follower already committed log #{db_seqnum} to database")
      server

    # ------------------------------------------------------- #
    # ------------------- CLIENT REQUESTS ------------------- #
    # ------------------------------------------------------- #
    # From: Leader >> To: Leader
    # Description: When client has a request.

    # If server is a leader, process client request
    { :CLIENT_REQUEST, m } when server.role == :LEADER ->
      server = server |> ClientRequest.handle_client_request(m)
      server

    # If server is not a leader (i.e. candidate/follower),
    # reply client that it is :NOT_LEADER and inform them of the current leader
    { :CLIENT_REQUEST, m } ->
     send m.clientP, {:CLIENT_REPLY, m.cid, :NOT_LEADER, server.leaderP, server.server_num}
     server

    # ------------------------------------------------------- #
    # --------------------- DB MESSAGES --------------------- #
    # ------------------------------------------------------- #
    # From: Database >> To: Server
    # Description: Informs the status of log replication in the database

    # If db replication was successful for leader
    { :DB_REPLY, _result, db_seqnum, client_request } when server.role == :LEADER ->
      server = ClientRequest.handle_database_reply(server, db_seqnum, client_request)
      server

    # If db replication was successful for follower/ candidate
    { :DB_REPLY, _result, db_seqnum, client_request } when server.last_applied < db_seqnum ->
      server = State.last_applied(server, db_seqnum)
      # IO.puts("Successfully committed log #{inspect (client_request)} to local database")
      server

    # Otherwise, ignore
    { :DB_REPLY, _result, _db_seqnum, client_request } ->
      # IO.puts("Ignore dbReply for log #{inspect (client_request)}")
      server

    # ------------------------------------------------------- #
    # ---------------------- UNEXPECTED --------------------- #
    # ------------------------------------------------------- #
    # Halt node if received unexpected messages

      unexpected ->
        Helper.node_halt("************* Server: unexpected message #{inspect unexpected}")

    end # receive

    server |> Server.next()
  end # next

  end # Server
