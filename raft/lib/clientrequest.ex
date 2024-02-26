
# distributed algorithms, n.dulay, 14 jan 2024
# coursework, raft consensus, v2

defmodule ClientRequest do

  # Handles client request and sends entries to all followers
  def handle_client_request(leader, request) do
    # Check if client request has been processed by leader
    request_status = check_request_status(leader, request.cid)

    # If client request has already been applied to database, send reply to client
    if request_status == :APPLIED_REQUEST do
      send request.clientP, { :CLIENT_REPLY, request.cid, request, leader.selfP, leader.server_num }
      leader
    end

    # If client request has not been applied to database or appended to leader's log
    leader = if request_status == :NEW_REQUEST do
      leader = Log.append_entry(leader, %{request: request, term: leader.curr_term})    # append client request to leader's log
      leader = State.commit_index(leader, Log.last_index(leader))                 # update the commit index for in the logs
      leader
    else
      leader
    end

    # If client request is a new request, send append entries request to other servers except itself
    for server <- leader.servers do
      if server != leader.selfP && request_status == :NEW_REQUEST do
        AppendEntries.send_entries_to_followers(leader, server)
        leader
      else
        leader
      end
    end
    leader
  end

  # Handles reply from database
  def handle_database_reply(leader, db_sequence_number, client_request) do
    # Update leader's last_applied value
    leader = leader |> State.last_applied(db_sequence_number)

    # Leader send reply to client that server has applied the request
    send client_request.clientP, { :CLIENT_REPLY, client_request.cid, client_request, leader.selfP, leader.server_num }

    # Leader broadcast to followers to commit the request to their local database
    for follower <- leader.servers do
      send follower, {:COMMIT_ENTRIES_REQUEST, db_sequence_number}
    end
    leader
  end

  # Checks the status of the client request
  def check_request_status(leader, client_id) do
    # Requests and client_ids that have been appended to log but not applied to database
    committed_log = Map.take(leader.log, Enum.to_list(1..leader.commit_index))
    committed_client_ids = for {_,entry} <- committed_log, do: entry.request.cid

    # Requests and client_ids that have been applied to database
    applied_log = Map.take(leader.log, Enum.to_list(1..leader.last_applied))
    applied_client_ids = for {_,entry} <- applied_log, do: entry.request.cid

    status = cond do
      # If leader log is empty, it is a new request, return :NEW_REQUEST
      Log.last_index(leader) == 0 ->
        :NEW_REQUEST

      # If client_id is in applied_client_ids, it has already been applied to database, return :APPLIED_REQUEST
      Enum.find(applied_client_ids, nil, fn entry -> entry == client_id end) != nil ->
        :APPLIED_REQUEST

      # If client_id is in committed_client_ids (but not in applied_client_ids), it has been appended to log, return :COMMITTED_REQUEST
      Enum.find(committed_client_ids, nil, fn entry -> entry == client_id end) != nil ->
        :COMMITTED_REQUEST

      true ->
        :NEW_REQUEST
    end
    status
  end

end
