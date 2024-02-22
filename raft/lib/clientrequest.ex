
# distributed algorithms, n.dulay, 14 jan 2024
# coursework, raft consensus, v2

defmodule ClientRequest do

# when client sends :CLIENT_REQUEST message to leader
def process_request_from_client(leader, client_request) do

  # check if the status has been proceessed by the leader
  status = check_request_status(leader, client_request.cid)

  # if the request has been appended to the database, then send a :CLIENT_REPLY back to client
  if status == :APPLIED_REQ do
    send client_request.clientP,{:CLIENT_REPLY, client_request.cid, client_request, leader.selfP, leader.server_num}
    leader
  end

  # if the request has not been appended to the database or committed to the log
  leader = if status == :NEW_REQ do
    leader = Log.apppend_entry(leader, %{request: client_request, term: leader.curr_term})
    leader = State.commit_index(leader, Log.last_index(leader))
    leader
  else
    leader
  end

  # if it is a new request, send it to the followers to append the entries
  for follower <- leader.servers do
    if follower != leader.selfP do
      AppendEntries.send_entries_to_follower(leader, follower)
      leader
    else
      leader
    end
  end

end

# when the database sends a :CLIENT_REQUEST to leader
def process_reply_from_db(leader, db_index, client_request) do

  # update leader's last_applied value
  leader = leader |> State.last_applied(db_index)

  # leader sends a reply to client that the server has processed the request
  send client_request.clientP, {:CLIENT_REPLY, client_request.cid, client_request, leader.selfP, leader.server_num}

  # leader broadcast to all followers to update their databases
  for follower <- leader.servers do
    send follower, {:COMMIT_ENTRIES_REQUEST, db_index}
  end
  leader
end

# check client request status in servers
def check_request_status(leader, cid) do

  # case 1: requeststhat have been appened to the log but not committed to the database
  committedLog = Map.take(leader.log, Enum.to_list(1..leader.commit_index))
  committedCid = for {_,entry} <- committedLog, do: entry.request.cid

  # case 2: request that have been committed to the database
  appliedLog = Map.take(leader.log, Enum.to_list(1..leader.last_applied))
  appliedCid = for {_,entry} <- appliedLog, do: entry.request.cid

  status = cond do
    # if the leader log is empty -> it is a new request. Therefore, return a new request
    Log.last_index(leader) == 0 -> :NEW_REQ

    # if cid is in applied cid, it has been appened to the database -> return :APPLIED_REQ
    Enum.find(appliedCid, nil, fn entry -> entry == cid end) != nil -> :APPLIED_REQ

    # if cid is in commited cid, it has been commited to the log but not database -> return :COMMITTED_REQ
    Enum.find(committedCid, nil, fn entry -> entry == cid end) != nil -> :COMMITTED_REQ

    # base case -> new req

    true -> :NEW_REQ
  end
  status
  
end


end # ClientRequest
