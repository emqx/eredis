%% @author:
%% @description:
-module(eredis_SUITE).
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include("eredis.hrl").

-compile([export_all, nowarn_export_all]).

-define(AUTH_PASS_ONLY_PASSWORD, "public").
-define(AUTH_USER_PASS_USERNAME, "test_user").
-define(AUTH_USER_PASS_PASSWORD, "test_passwd").

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------
all() ->
    [
        {group, tcp},
        {group, ssl},
        sentinel_auth_test,
        sentinel_auth_fallback_test,
        sentinel_empty_binary_password_test,
        sentinel_ping_timeout_test,
        fake_sentinel_resp_framing_test,
        fake_sentinel_stop_test
    ].

groups() ->
    Cases = [auth_test,
             get_set_test,
             delete_test,
             mset_mget_test,
             exec_test,
             exec_nil_test,
             pipeline_test,
             pipeline_mixed_test,
             q_noreply_test,
             q_async_test,
             socket_closed_test],
    AuthGroups = [{group, username_password}, {group, password_only}],
    [
        {ssl, AuthGroups},
        {tcp, AuthGroups},
        {username_password, Cases},
        {password_only, Cases}
    ].
init_per_suite(_Cfg) ->
    _Cfg.

end_per_suite(_) ->
    ok.

init_per_group(Group , Cfg) when Group =:= tcp; Group =:= ssl ->
    [{t, Group}| Cfg];
init_per_group(Group , Cfg) when Group =:= password_only; Group =:= username_password ->
    [{auth_type, Group}| Cfg];
init_per_group(_Group , Cfg) ->
    Cfg.

end_per_group(_Group, _Cfg) ->
    ok.
%%--------------------------------------------------------------------

maybe_credential(password_only) ->
    ?AUTH_PASS_ONLY_PASSWORD;
maybe_credential(username_password) ->
    eredis:make_credentials(?AUTH_USER_PASS_USERNAME, ?AUTH_USER_PASS_PASSWORD).

connect_ssl(Authtype, DataDir) ->
    Options = [{ssl_options, [{cacertfile, DataDir ++ "certs/ca.crt"},
                              {certfile, DataDir ++ "certs/redis.crt"},
                              {keyfile, DataDir ++ "certs/redis.key"},
                              %% Hostname check is enabled by default in Erlang/OTP 26.
                              {verify, verify_none}]},
               {tcp_options ,[]}],
    MaybeCredentials = maybe_credential(Authtype),
    {ok, SSLClient} =
        eredis:start_link("127.0.0.1", 6378, 0, MaybeCredentials, 3000, 5000, Options),
    SSLClient.

connect_tcp(Authtype) ->
    MaybeCredentials = maybe_credential(Authtype),
    {ok, TcpClient} = eredis:start_link("127.0.0.1", 6379, 0, MaybeCredentials),
    TcpClient.

c(Config) ->
    AuthType = ?config(auth_type, Config),
    case ?config(t, Config) of
        ssl ->
            DataDir = ?config(data_dir, Config),
            C = connect_ssl(AuthType, DataDir),
            eredis:q(C, ["flushdb"]),
            C;
        _ ->
            C = connect_tcp(AuthType),
            eredis:q(C, ["flushdb"]),
            C
    end.

auth_test(Config) ->
    C = c(Config),
    ?assertEqual({ok, <<"OK">>}, eredis:q(C, ["AUTH", ?AUTH_PASS_ONLY_PASSWORD])),
    ?assertEqual({ok, <<"OK">>}, eredis:q(C, ["AUTH", ?AUTH_USER_PASS_USERNAME, ?AUTH_USER_PASS_PASSWORD])),
    ?assertEqual(
        {error,<<"WRONGPASS invalid username-password pair or user is disabled.">>},
        eredis:q(C, ["AUTH", "wrong_password"])),
    ?assertEqual(
        {error,<<"WRONGPASS invalid username-password pair or user is disabled.">>},
        eredis:q(C, ["AUTH", ?AUTH_USER_PASS_USERNAME, "wrong_password"])).

get_set_test(Config) ->
    C = c(Config),
    ?assertMatch({ok, _}, eredis:q(C, ["DEL", foo])),
    ?assertEqual({ok, undefined}, eredis:q(C, ["GET", foo])),
    ?assertEqual({ok, <<"OK">>}, eredis:q(C, ["SET", foo, bar])),
    ?assertEqual({ok, <<"bar">>}, eredis:q(C, ["GET", foo])).

delete_test(Config) ->
    C = c(Config),
    ?assertMatch({ok, _}, eredis:q(C, ["DEL", foo])),

    ?assertEqual({ok, <<"OK">>}, eredis:q(C, ["SET", foo, bar])),
    ?assertEqual({ok, <<"1">>}, eredis:q(C, ["DEL", foo])),
    ?assertEqual({ok, undefined}, eredis:q(C, ["GET", foo])).

mset_mget_test(Config) ->
    C = c(Config),
    Keys = lists:seq(1, 10),

    ?assertMatch({ok, _}, eredis:q(C, ["DEL" | Keys])),

    KeyValuePairs = [[K, K*2] || K <- Keys],
    ExpectedResult = [list_to_binary(integer_to_list(K * 2)) || K <- Keys],

    ?assertEqual({ok, <<"OK">>}, eredis:q(C, ["MSET" | lists:flatten(KeyValuePairs)])),
    ?assertEqual({ok, ExpectedResult}, eredis:q(C, ["MGET" | Keys])),
    ?assertMatch({ok, _}, eredis:q(C, ["DEL" | Keys])).

exec_test(Config) ->
    C = c(Config),

    ?assertMatch({ok, _}, eredis:q(C, ["LPUSH", "k1", "b"])),
    ?assertMatch({ok, _}, eredis:q(C, ["LPUSH", "k1", "a"])),
    ?assertMatch({ok, _}, eredis:q(C, ["LPUSH", "k2", "c"])),

    ?assertEqual({ok, <<"OK">>}, eredis:q(C, ["MULTI"])),
    ?assertEqual({ok, <<"QUEUED">>}, eredis:q(C, ["LRANGE", "k1", "0", "-1"])),
    ?assertEqual({ok, <<"QUEUED">>}, eredis:q(C, ["LRANGE", "k2", "0", "-1"])),

    ExpectedResult = [[<<"a">>, <<"b">>], [<<"c">>]],

    ?assertEqual({ok, ExpectedResult}, eredis:q(C, ["EXEC"])),

    ?assertMatch({ok, _}, eredis:q(C, ["DEL", "k1", "k2"])).

exec_nil_test(Config) ->
    C1 = c(Config),
    C2 = c(Config),

    ?assertEqual({ok, <<"OK">>}, eredis:q(C1, ["WATCH", "x"])),
    ?assertMatch({ok, _}, eredis:q(C2, ["INCR", "x"])),
    ?assertEqual({ok, <<"OK">>}, eredis:q(C1, ["MULTI"])),
    ?assertEqual({ok, <<"QUEUED">>}, eredis:q(C1, ["GET", "x"])),
    ?assertEqual({ok, undefined}, eredis:q(C1, ["EXEC"])),
    ?assertMatch({ok, _}, eredis:q(C1, ["DEL", "x"])).

pipeline_test(Config) ->
    C = c(Config),

    P1 = [["SET", a, "1"],
          ["LPUSH", b, "3"],
          ["LPUSH", b, "2"]],

    ?assertEqual([{ok, <<"OK">>}, {ok, <<"1">>}, {ok, <<"2">>}],
                 eredis:qp(C, P1)),

    P2 = [["MULTI"],
          ["GET", a],
          ["LRANGE", b, "0", "-1"],
          ["EXEC"]],

    ?assertEqual([{ok, <<"OK">>},
                  {ok, <<"QUEUED">>},
                  {ok, <<"QUEUED">>},
                  {ok, [<<"1">>, [<<"2">>, <<"3">>]]}],
                 eredis:qp(C, P2)),

    ?assertMatch({ok, _}, eredis:q(C, ["DEL", a, b])).

pipeline_mixed_test(Config) ->
    C = c(Config),
    P1 = [["LPUSH", c, "1"] || _ <- lists:seq(1, 100)],
    P2 = [["LPUSH", d, "1"] || _ <- lists:seq(1, 100)],
    Expect = [{ok, list_to_binary(integer_to_list(I))} || I <- lists:seq(1, 100)],
    spawn(fun () ->
                  erlang:yield(),
                  ?assertEqual(Expect, eredis:qp(C, P1))
          end),
    spawn(fun () ->
                  ?assertEqual(Expect, eredis:qp(C, P2))
          end),
    timer:sleep(10),
    ?assertMatch({ok, _}, eredis:q(C, ["DEL", c, d])).

q_noreply_test(Config) ->
    C = c(Config),
    ?assertEqual(ok, eredis:q_noreply(C, ["GET", foo])),
    ?assertEqual(ok, eredis:q_noreply(C, ["SET", foo, bar])),
    %% Even though q_noreply doesn't wait, it is sent before subsequent requests:
    ?assertEqual({ok, <<"bar">>}, eredis:q(C, ["GET", foo])).
q_async_test(Config) ->
    C = c(Config),
    ?assertEqual({ok, <<"OK">>}, eredis:q(C, ["SET", foo, bar])),
    ?assertEqual(ok, eredis:q_async(C, ["GET", foo], self())),
    receive
        {response, Msg} ->
            ?assertEqual(Msg, {ok, <<"bar">>}),
            ?assertMatch({ok, _}, eredis:q(C, ["DEL", foo]))
    end.

undefined_database_test() ->
    ?assertMatch({ok,_}, eredis:start_link("localhost", 6379, undefined)).

sentinel_auth_test(_Config) ->
    {Port, Acceptor} = start_fake_sentinel(auth_required),
    {ok, C} = eredis_sentinel_client:start_link("127.0.0.1", Port, [{password, "public"}]),
    ?assertEqual({ok, {"127.0.0.1", 6379}}, eredis_sentinel_client:get_master(C, mymaster)),
    eredis_sentinel_client:stop(C),
    Acceptor ! stop,
    ok.

sentinel_auth_fallback_test(_Config) ->
    {Port, Acceptor} = start_fake_sentinel(auth_disabled),
    {ok, C} = eredis_sentinel_client:start_link("127.0.0.1", Port, [{password, "public"}]),
    ?assertEqual({ok, {"127.0.0.1", 6379}}, eredis_sentinel_client:get_master(C, mymaster)),
    eredis_sentinel_client:stop(C),
    Acceptor ! stop,
    ok.

sentinel_empty_binary_password_test(_Config) ->
    {Port, Acceptor} = start_fake_sentinel(ping_forbidden),
    {ok, C} = eredis_sentinel_client:start_link("127.0.0.1", Port, [{password, <<>>}]),
    ?assertEqual({ok, {"127.0.0.1", 6379}}, eredis_sentinel_client:get_master(C, mymaster)),
    eredis_sentinel_client:stop(C),
    Acceptor ! stop,
    ok.

sentinel_ping_timeout_test(_Config) ->
    {Port, Acceptor} = start_fake_sentinel(ping_timeout),
    Result = (catch eredis_sentinel_client:start_link("127.0.0.1", Port, [{password, "public"}])),
    ?assertMatch({error, #{type := connection_error, reason := timeout}}, Result),
    Acceptor ! stop,
    ok.

fake_sentinel_resp_framing_test(_Config) ->
    {Port, Acceptor} = start_fake_sentinel(auth_required),
    {ok, Socket} = gen_tcp:connect({127, 0, 0, 1}, Port, [binary, {active, false}, {packet, raw}]),
    Ping = iolist_to_binary(eredis:create_multibulk(["PING"])),
    {PingHead, PingTail} = split_binary(Ping, 3),
    ok = gen_tcp:send(Socket, PingHead),
    ?assertEqual({error, timeout}, gen_tcp:recv(Socket, 0, 50)),
    ok = gen_tcp:send(Socket, PingTail),
    ?assertEqual(<<"-NOAUTH Authentication required.\r\n">>, recv_until(Socket, <<"\r\n">>, 1000)),
    Auth = iolist_to_binary(eredis:create_multibulk(["AUTH", "public"])),
    GetMaster = iolist_to_binary(eredis:create_multibulk(["SENTINEL", "get-master-addr-by-name", "mymaster"])),
    ok = gen_tcp:send(Socket, <<Auth/binary, GetMaster/binary>>),
    ?assertEqual(<<"+OK\r\n*2\r\n$9\r\n127.0.0.1\r\n$4\r\n6379\r\n">>,
                 recv_until(Socket, <<"6379\r\n">>, 1000)),
    gen_tcp:close(Socket),
    Acceptor ! stop,
    ok.

fake_sentinel_stop_test(_Config) ->
    {Port, Acceptor} = start_fake_sentinel(auth_disabled),
    Ref = erlang:monitor(process, Acceptor),
    {ok, Socket} = gen_tcp:connect({127, 0, 0, 1}, Port, [binary, {active, false}, {packet, raw}]),
    Acceptor ! stop,
    ?assertEqual(ok, wait_for_process_down(Ref, Acceptor, 1000)),
    gen_tcp:close(Socket),
    ok.

socket_closed_test(Config) ->
    C = c(Config),
    Header = case proplists:get_value(t, Config) of
                 ssl -> ssl_closed;
                 tcp -> tcp_closed
             end,

    DoSend = fun(H) when H =:= ssl_closed; H =:= tcp_closed ->
                     C ! {H, fake_socket};
                (Cmd) ->
                     eredis:q(C, Cmd)
             end,
    %% attach an id to each message for later
    Msgs = [{1, ["GET", "foo"]},
            {2, ["GET", "bar"]},
            {3, Header}],
    Pids = [ remote_query(DoSend, M) || M <- Msgs ],
    Results = gather_remote_queries(Pids),
    ?assertEqual({error, Header}, proplists:get_value(1, Results)),
    ?assertEqual({error, Header}, proplists:get_value(2, Results)).

remote_query(Fun, {Id, Cmd}) ->
    Parent = self(),
    spawn(fun() ->
                  Result = Fun(Cmd),
                  Parent ! {self(), Id, Result}
          end).

gather_remote_queries(Pids) ->
    gather_remote_queries(Pids, []).

gather_remote_queries([], Acc) ->
    Acc;
gather_remote_queries([Pid | Rest], Acc) ->
    receive
        {Pid, Id, Result} ->
            gather_remote_queries(Rest, [{Id, Result} | Acc])
    after
        10000 ->
            error({gather_remote_queries, timeout})
    end.

wait_for_process_down(Ref, Pid, Timeout) ->
    receive
        {'DOWN', Ref, process, Pid, _Reason} ->
            ok
    after Timeout ->
        Pid ! stop,
        error({process_still_alive, Pid})
    end.

start_fake_sentinel(Mode) ->
    {ok, Listen} = gen_tcp:listen(0, [binary, {active, false}, {packet, raw}, {ip, {127, 0, 0, 1}}]),
    {ok, Port} = inet:port(Listen),
    Acceptor = spawn_link(fun() -> fake_sentinel_accept(Listen, Mode) end),
    {Port, Acceptor}.

fake_sentinel_accept(Listen, Mode) ->
    receive
        stop ->
            gen_tcp:close(Listen)
    after 0 ->
        case gen_tcp:accept(Listen, 100) of
            {ok, Socket} ->
                case fake_sentinel_loop(Listen, Socket, Mode, initial_auth_state(Mode), <<>>) of
                    stopped -> ok;
                    ok -> fake_sentinel_accept(Listen, Mode)
                end;
            {error, timeout} ->
                fake_sentinel_accept(Listen, Mode)
        end
    end.

initial_auth_state(auth_required) ->
    false;
initial_auth_state(_) ->
    true.

fake_sentinel_loop(Listen, Socket, Mode, Authed, Buffer) ->
    receive
        stop ->
            gen_tcp:close(Socket),
            gen_tcp:close(Listen),
            stopped
    after 0 ->
        case gen_tcp:recv(Socket, 0, 100) of
            {ok, Data} ->
                handle_fake_sentinel_data(Listen, Socket, Mode, Authed, <<Buffer/binary, Data/binary>>);
            {error, timeout} ->
                fake_sentinel_loop(Listen, Socket, Mode, Authed, Buffer);
            {error, closed} ->
                ok
        end
    end.

handle_fake_sentinel_data(Listen, Socket, Mode, Authed, Buffer) ->
    case parse_resp_commands(Buffer) of
        {ok, Commands, Rest} ->
            case handle_fake_sentinel_commands(Socket, Mode, Authed, Commands) of
                {continue, Authed1} ->
                    fake_sentinel_loop(Listen, Socket, Mode, Authed1, Rest);
                close ->
                    gen_tcp:close(Socket)
            end;
        more ->
            fake_sentinel_loop(Listen, Socket, Mode, Authed, Buffer)
    end.

handle_fake_sentinel_commands(_Socket, _Mode, Authed, []) ->
    {continue, Authed};
handle_fake_sentinel_commands(Socket, Mode, Authed, [Command | Rest]) ->
    case handle_fake_sentinel_command(Socket, Mode, Authed, Command) of
        {continue, Authed1} ->
            handle_fake_sentinel_commands(Socket, Mode, Authed1, Rest);
        close ->
            close
    end.

handle_fake_sentinel_command(Socket, ping_forbidden, Authed, [<<"PING">>]) ->
    ok = gen_tcp:send(Socket, <<"-ERR PING should not be sent.\r\n">>),
    {continue, Authed};
handle_fake_sentinel_command(_Socket, ping_timeout, Authed, [<<"PING">>]) ->
    {continue, Authed};
handle_fake_sentinel_command(Socket, _Mode, true, [<<"PING">>]) ->
    ok = gen_tcp:send(Socket, <<"+PONG\r\n">>),
    {continue, true};
handle_fake_sentinel_command(Socket, auth_required, _Authed, [<<"AUTH">>, <<"public">>]) ->
    ok = gen_tcp:send(Socket, <<"+OK\r\n">>),
    {continue, true};
handle_fake_sentinel_command(Socket, _Mode, _Authed, [<<"AUTH">>, <<"public">>]) ->
    ok = gen_tcp:send(Socket, <<"-ERR AUTH <password> called without any password configured for the default user.\r\n">>),
    close;
handle_fake_sentinel_command(Socket, _Mode, true, [<<"SENTINEL">>, <<"get-master-addr-by-name">>, <<"mymaster">>]) ->
    ok = gen_tcp:send(Socket, <<"*2\r\n$9\r\n127.0.0.1\r\n$4\r\n6379\r\n">>),
    {continue, true};
handle_fake_sentinel_command(Socket, _Mode, Authed, _Command) ->
    ok = gen_tcp:send(Socket, <<"-NOAUTH Authentication required.\r\n">>),
    {continue, Authed}.

parse_resp_commands(Data) ->
    parse_resp_commands(Data, []).

parse_resp_commands(<<>>, Acc) ->
    {ok, lists:reverse(Acc), <<>>};
parse_resp_commands(Data, Acc) ->
    case parse_resp_command(Data) of
        {ok, Command, Rest} ->
            parse_resp_commands(Rest, [Command | Acc]);
        more when Acc =:= [] ->
            more;
        more ->
            {ok, lists:reverse(Acc), Data}
    end.

parse_resp_command(Data) ->
    case split_resp_line(Data) of
        {ok, <<"*", CountBin/binary>>, Rest} ->
            parse_resp_args(Rest, binary_to_integer(CountBin), []);
        more ->
            more
    end.

parse_resp_args(Rest, 0, Acc) ->
    {ok, lists:reverse(Acc), Rest};
parse_resp_args(Data, Count, Acc) ->
    case split_resp_line(Data) of
        {ok, <<"$", SizeBin/binary>>, Rest} ->
            Size = binary_to_integer(SizeBin),
            case Rest of
                <<Arg:Size/binary, "\r\n", Tail/binary>> ->
                    parse_resp_args(Tail, Count - 1, [Arg | Acc]);
                _ ->
                    more
            end;
        more ->
            more
    end.

split_resp_line(Data) ->
    case binary:match(Data, <<"\r\n">>) of
        {Pos, 2} ->
            Line = binary:part(Data, 0, Pos),
            Rest = binary:part(Data, Pos + 2, byte_size(Data) - Pos - 2),
            {ok, Line, Rest};
        nomatch ->
            more
    end.

recv_until(Socket, Pattern, Timeout) ->
    recv_until(Socket, Pattern, Timeout, <<>>).

recv_until(Socket, Pattern, Timeout, Acc) ->
    case binary:match(Acc, Pattern) of
        nomatch ->
            {ok, Data} = gen_tcp:recv(Socket, 0, Timeout),
            recv_until(Socket, Pattern, Timeout, <<Acc/binary, Data/binary>>);
        _ ->
            Acc
    end.
