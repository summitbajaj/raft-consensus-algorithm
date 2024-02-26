
# distributed algorithms, n.dulay, 14 jan 2024
# coursework, raft consensus, v2

defmodule Client do

  # client = client process state (client.f. self/this)

  # _________________________________________________________ Client setters()
  def seqnum(client, v),  do: Map.put(client, :seqnum, v)
  def request(client, v), do: Map.put(client, :request, v)
  def result(client, v),  do: Map.put(client, :result, v)
  def leaderP(client, v), do: Map.put(client, :leaderP, v)
  def servers(client, v), do: Map.put(client, :servers, v)

  # _________________________________________________________ Client.start()
  def start(config, client_num, servers) do
    config = config
      |> Configuration.node_info("Client", client_num)
      |> Debug.node_starting()

    client = %{                              # initialise client state variables
      config:     config,
      client_num: client_num,
      clientP:    self(),
      servers:    servers,
      leaderP:    nil,
      seqnum:     0,
      request:    nil,
      result:     nil,
    }

    Process.send_after(self(), :CLIENT_TIMELIMIT, config.client_timelimit)

    Process.sleep(config.election_timeout_range.first)  # wait until first elections run



    client |> Client.next()
  end # start

  # _________________________________________________________ Client.next()
  def next(client) do
    if client.seqnum == client.config.max_client_requests do          # all done
      Helper.node_sleep("Client #{client.client_num} all requests completed = #{client.seqnum}")
    end # if

    receive do
    { :CLIENT_TIMELIMIT } ->
      Helper.node_sleep("  Client #{client.client_num}, client timelimit reached, tent = #{client.seqnum}")

    after client.config.client_request_interval ->

      account1 = Enum.random 1 .. client.config.n_accounts         # from account
      account2 = Enum.random 1 .. client.config.n_accounts         # to account
      amount   = Enum.random 1 .. client.config.max_amount

      client   = Client.seqnum(client, client.seqnum + 1)
      cmd = { :MOVE, amount, account1, account2 }
      cid = { client.client_num, client.seqnum }                        # unique client id for cmd

      client
      |> Client.request({ :CLIENT_REQUEST, %{clientP: client.clientP, cid: cid, cmd: cmd } })
      |> Client.send_client_request_receive_reply(cid)
      |> Client.next()

    end # receive
  end # next

  # _________________________________________________________ send_client_request_receive_reply()
  def send_client_request_receive_reply(client, cid) do
    client
    |> Client.send_client_request_to_leader()
    |> Client.receive_reply_from_leader(cid)
  end # send_client_request_receive_reply

  # _________________________________________________________ send_client_request_to_leader()
  def send_client_request_to_leader(client) do
    # IO.puts("At round robin!")
    client = if client.leaderP do client else      # round-robin leader selection
      [server | rest] = client.servers
      client = client |> Client.leaderP(server)
                      |> Client.servers(rest ++ [server])
      client
    end # if
    send client.leaderP, client.request
    client
  end # send_client_request_to_leader

  # _________________________________________________________ receive_reply_from_leader()
  def receive_reply_from_leader(client, cid) do
    receive do
    { :CLIENT_REPLY, m_cid, :NOT_LEADER, leaderP, leader_num} when m_cid == cid ->
      Process.sleep(200)
      client |> Client.leaderP(leaderP)
        |> Client.send_client_request_receive_reply(cid)

    { :CLIENT_REPLY, m_cid, reply, leaderP, leader_num } when m_cid == cid ->
      client = client
        |> Client.result(reply)
        |> Client.leaderP(leaderP)
        |> Monitor.send_msg({ :CLIENT_REQUEST, leader_num })
      client

    { :CLIENT_REPLY, m_cid, _reply, _leaderP, _leaderNum } when m_cid < cid ->
      client |> Client.receive_reply_from_leader(cid)

    { :CLIENT_TIMELIMIT } ->
      Helper.node_sleep("  Client #{client.client_num}, client timelimit reached, sent = #{client.seqnum}")

    unexpected ->
      Helper.node_halt("***************** Client: unexpected message #{inspect unexpected}")

    after client.config.client_reply_timeout ->

      # leader probably crashed, retry with next server
      client |> Client.leaderP(nil)
        |> Client.send_client_request_receive_reply(cid)
    end # receive
  end # receive_reply

  end # Client
