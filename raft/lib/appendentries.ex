
# distributed algorithms, n.dulay, 14 jan 2024
# coursework, raft consensus, v2

defmodule AppendEntries do

  # leader sends append entries request to followers
  def send_entries_to_follower(leader, followerP) do

    # case 1: follower's nextIndex is less than leader's last log index + 1 -> append new entries
    if leader.next_index[followerP] < (Log.last_index(leader)+1) do

      followerPrevEntryIndex = leader.next_index[followerP]-1
      entries = Log.get_entries(leader, (followerPrevEntryIndex+1)..Log.last_index(leader))
      prevTerm = Log.term_at(leader, followerPrevEntryIndex)

      # send the append request to followers
      Timer.restart_append_entries_timer(leader, followerP)
      send followerP,{:APPEND_ENTRIES_REQUEST, leader.curr_term, followerPrevEntryIndex, prevTerm, entries, leader.commit_index}
      leader

    # case 2: follower's nextIndex is equal to leader's last log index + 1 -> no new entry to append
    # -> send dummy heartbeats
    else
      Timer.restart_append_entries_timer(leader, followerP)
      send followerP, {:APPEND_ENTRIES_REQUEST, leader.curr_term, leader.commit_index}
      leader
    end
    leader
  end

  # when server receives an append entries request
  def receive_append_entries_request(
  s, # leader that sends the request
  leaderTerm, # current term of the leader
  prevIndex, # index of the log entry immediately preceding new ones
  prevTerm, # term of prevIndex entry
  leaderEntries, # log entries to be appended
  commitIndex # leader's commit_index
  ) do

    # case 1: if a leader receives a request from another server whose term is greater, step down
    s =  if s.role != :FOLLOWER && s.curr_term < leaderTerm do
      s = Vote.stepdown(s, leaderTerm)
    else
      s
    end

    # case 2: if current term larger than the leader's term, reject the request
    if s.curr_term > leaderTerm do
      send s.leaderP, {:APPEND_ENTRIES_REPLY, s.selfP, s.curr_term, false, nil}
    end

    # case 3: if current term is equal to leader's term -> check if it has been successfully appended
    success = (s.curr_term == leaderTerm) && (prevIndex == 0) || (prevIndex <= Log.last_index(s) && Log.term_at(s, prevIndex) == prevTerm)

    s = if success do
      s = storeEntries(s, prevIndex, leaderEntries, commitIndex)
    else
      s
    end

    if s.curr_term == leaderTerm do
      send s.leaderP, {:APPEND_ENTRIES_REPLY, s.selfP, s.curr_term, success, s.commit_index}
    end

  end

  # when server receives an append entries request from the leader
  def storeEntries(
    s,            # The leader that sends the request
    prevIndex,    # The index of log entry immediately preceding new ones
    entries,      # The log entries to store
    commitIndex   # The index of highest log entry known to be committed
  ) do

    # find the starting point to append the entries
    entryPointList = for {index, entry} <- entries do
      if Log.last_index(s) >= index do
        if s.log[index].term != entry.term do
          index
        else
          nil
        end
      else
        index
      end
    end

    entryPoint = Enum.min(entryPointList)

    #  delete irrelevant entries before the entryPoint
    entries = if entryPoint != nil do
      Map.drop(entries, Enum.to_list(0..(entryPoint-1)))
    else
      %{}
    end

    s = if entryPoint != nil && entryPoint < Log.last_index(s) do
      s = s|> Log.delete_entries_from(entryPoint)
      s
    else
      s
    end

    # merge the logs from the entryPoint
    s = if entries != %{} do
      s = Log.merge_entries(s, entries)
      s = State.commit_index(s, Log.last_index(s))

    else
      s
    end
    s
  end

  # when a leader receives follower's reply for appending entries request
  # in this situation the followers term is lower or equal to the leaders term
  def receive_append_entries_reply_from_follower(s, followerP, followerTerm, success, followerLastIndex) do

    # update leader's next_index tracker with follower when consistency is succesful
    s = if success do
      s = State.next_index(s, followerP, followerLastIndex+1)
      s

  # Decrement follower's next_index when consistency check fails
    else
        s = State.next_index(s, followerP, max(s.next_index[followerP]-1, 1))
        send_entries_to_followers(s, followerP)
        s
    end

    # check if majority of followers have replicated the log
    checker = for follower <- (s.last_applied+1)..Log.last_index(s) // 1,
      into: Map.new()
      do
        {follower, Enum.reduce(s.servers, 1, fn followerP, count ->
          if followerP != s.selfP && s.next_index[followerP] > follower do
            count + 1
          else
            count
          end
        end)}
      end

      for {index, c} <- counter do
        if c >= s.majority do
          send s.databaseP, {:DB_REQUEST, Log.request_at(s, index), index}
        end
       end
       s
      end

end # AppendEntries
