# distributed algorithms, n.dulay, 14 jan 2024
# coursework, raft consensus, v2

defmodule AppendEntries do

  # Sends entries to followers
  def send_entries_to_followers(leader_server, follower_process_id) do
    # If follower's next_index is less than leader's log length + 1, append new entries
    # to simulate slow leader change
    # Process.sleep(100)
    if leader_server.next_index[follower_process_id] < (Log.last_index(leader_server) + 1) do
      follower_prev_index = leader_server.next_index[follower_process_id] - 1
      entries_to_send = Log.get_entries(leader_server, (follower_prev_index+1)..Log.last_index(leader_server))
      previous_term = Log.term_at(leader_server, follower_prev_index)

      # Restart timer and send append entries request to follower
      Timer.restart_append_entries_timer(leader_server, follower_process_id)
      send follower_process_id, { :APPEND_ENTRIES_REQUEST, leader_server.curr_term, follower_prev_index, previous_term, entries_to_send, leader_server.commit_index}
      leader_server

    # If follower is already up-to-date, leader sends dummy heartbeats to follower
    else
      Timer.restart_append_entries_timer(leader_server, follower_process_id)
      send follower_process_id, { :APPEND_ENTRIES_REQUEST, leader_server.curr_term, leader_server.commit_index }
      leader_server
    end
    leader_server
  end

  # Handles the receipt of append entries request
  def receive_append_entries_request(server, leader_term, prev_index, prev_term, leader_entries, commit_index) do
    # If server is a candidate/leader but received an append entries request from someone with a larger term, stepdown
    server = if server.role != :FOLLOWER && server.curr_term < leader_term do
      Vote.stepdown(server, leader_term)
    else
      server
    end

    # If server's current term is larger than the leader's, reject leader
    if server.curr_term > leader_term do
      send server.leaderP, {:APPEND_ENTRIES_REPLY, server.selfP, server.curr_term, false, nil}
    end

    # If server's current term equals leader's term, check if append is successful
    append_success = (server.curr_term == leader_term) && (prev_index == 0 || (prev_index <= Log.last_index(server) && prev_term == Log.term_at(server, prev_index)))

    server =
      if append_success do
        store_entries(server, prev_index, leader_entries, commit_index)
      else
        server
      end

    if server.curr_term == leader_term do
      send server.leaderP, {:APPEND_ENTRIES_REPLY, server.selfP, server.curr_term, append_success, server.commit_index}
    end

    server
  end

  # Stores entries received from leader
  def store_entries(server, prev_index, entries, commit_index) do
    # Find the point in the entry to start appending
    break_point_list = for {index, entry} <- entries do
      if Log.last_index(server) >= index do
        if server.log[index].term != entry.term do
          index
        else
          nil
        end
      else
        index
      end
    end

    break_point = Enum.min(break_point_list) # the index where the server's log and entries start to diverge

    # Delete extraneous entries
    entries = if break_point != nil do
      Map.drop(entries, Enum.to_list(0..(break_point-1)))  # delete the entries before breakPoint (which have been appended to follower's log)
    else
      %{}
    end

    server = if break_point != nil && break_point < Log.last_index(server) do
      server = server|>
      Log.delete_entries_from(break_point)               # delete entries from the point where diverge with leader
      server
    else
      server
    end

    # Merge logs from breakPoint onwards
    server = if entries != %{} do
      server = Log.merge_entries(server, entries)                 # append missing entries
      server = State.commit_index(server, Log.last_index(server))      # update commit index (for logs)
      server
    else
      server
    end
    server
  end

  # Handles the receipt of append entries reply from follower
  def receive_append_entries_reply_from_follower(server, follower_process_id, follower_term, append_success, follower_last_index) do
    # Update leader's next_index tracker with follower when consistency check success
    server = if append_success do
      State.next_index(server, follower_process_id, follower_last_index+1)
    # Decrement follower's next_index when consistency check fails
    else
      State.next_index(server, follower_process_id, max(server.next_index[follower_process_id]-1, 1))
      send_entries_to_followers(server, follower_process_id)
      server
    end

    # Check if majority of servers have committed request to log
    commit_counter = for i <- (server.last_applied+1)..Log.last_index(server) // 1,
    into: Map.new
    do
    {i, Enum.reduce(server.servers, 1, fn follower_process_id, count ->
      if follower_process_id != server.selfP && server.next_index[follower_process_id] > i do
        count + 1
      else
        count
      end
    end)}
    end

    for {index, count} <- commit_counter do
    # If leader received majority reply from followers, notify the local database to store the request
    if count >= server.majority do
      send server.databaseP, {:DB_REQUEST, Log.request_at(server, index), index}
    end
    end
    server
  end

end
