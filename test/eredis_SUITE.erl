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
        sentinel_auth_matrix_test,
        sentinel_separate_credentials_test,
        eredis_application_starts_no_sentinel_resources_test,
        eredis_sentinel_resources_start_lazily_test,
        stop_missing_sentinel_manager_does_not_start_resources_test,
        eredis_sentinel_sup_legacy_start_link_test,
        eredis_sentinel_sup_replaces_stopped_child_spec_test,
        sentinel_registry_survives_first_starter_exit_test,
        sentinel_registry_management_api_test,
        sentinel_manager_ref_isolates_sentinel_state_test,
        sentinel_manager_stop_api_test,
        start_link_args_without_sentinel_host_port_test,
        start_link_args_without_sentinel_servers_test,
        sentinel_credentials_not_inferred_test,
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

sentinel_auth_matrix_test(_Config) ->
    Cases = [
        #{
            name => no_master_no_sentinel_password,
            redis => no_auth,
            sentinel => auth_disabled,
            args => []
        },
        #{
            name => master_password_only,
            redis => "redis-password",
            sentinel => auth_disabled,
            args => [{password, "redis-password"}]
        },
        #{
            name => sentinel_password_only,
            redis => no_auth,
            sentinel => {auth_required, "sentinel-password"},
            args => [{sentinel_password, "sentinel-password"}]
        },
        #{
            name => master_and_sentinel_passwords,
            redis => "redis-password",
            sentinel => {auth_required, "sentinel-password"},
            args => [
                {password, "redis-password"},
                {sentinel_password, "sentinel-password"}
            ]
        }
    ],
    lists:foreach(fun assert_sentinel_connection/1, Cases),
    ok.

sentinel_separate_credentials_test(_Config) ->
    RedisPassword = "redis-password",
    SentinelUsername = "sentinel-user",
    SentinelPassword = "sentinel-password",
    {RedisPort, RedisAcceptor} = start_fake_redis(RedisPassword),
    {SentinelPort, SentinelAcceptor} = start_fake_sentinel(
        {auth_required, SentinelUsername, SentinelPassword},
        RedisPort
    ),
    try
        {ok, C} = eredis:start_link(
            sentinel_args(SentinelPort, RedisPassword, [
                {sentinel_username, SentinelUsername},
                {sentinel_password, SentinelPassword}
            ])
        ),
        ?assertEqual({ok, <<"PONG">>}, eredis:q(C, ["PING"])),
        eredis:stop(C)
    after
        cleanup_sentinel_test(RedisAcceptor, SentinelAcceptor)
    end,
    ok.

eredis_application_starts_no_sentinel_resources_test(_Config) ->
    catch eredis_sentinel:stop(),
    catch application:stop(eredis),
    catch eredis_sentinel_registry:stop(),
    catch application:stop(gproc),
    try
        {ok, _Started} = application:ensure_all_started(eredis),
        ?assert(is_pid(whereis(eredis_sup))),
        ?assertEqual(undefined, whereis(eredis_sentinel)),
        ?assertEqual(undefined, whereis(eredis_sentinel_sup)),
        ?assert(is_pid(whereis(gproc)))
    after
        catch application:stop(eredis),
        catch application:stop(gproc)
    end,
    ok.

eredis_sentinel_resources_start_lazily_test(_Config) ->
    catch application:stop(eredis),
    catch eredis_sentinel_registry:stop(),
    catch application:stop(gproc),
    try
        {ok, _Started} = application:ensure_all_started(eredis),
        ?assertEqual(undefined, whereis(eredis_sentinel_sup)),
        ?assert(is_pid(whereis(gproc))),

        {ok, Pid} =
            eredis_sentinel_sup:start_child(
                [{"127.0.0.1", 26379}], [], {eredis_SUITE, lazy_resources}
            ),
        try
            ?assert(is_pid(whereis(eredis_sentinel_sup))),
            ?assert(is_pid(whereis(gproc))),
            ?assertEqual(Pid, eredis_sentinel_registry:whereis_name({eredis_SUITE, lazy_resources}))
        after
            catch eredis_sentinel:stop({eredis_SUITE, lazy_resources})
        end
    after
        catch application:stop(eredis),
        catch application:stop(gproc)
    end,
    ok.

stop_missing_sentinel_manager_does_not_start_resources_test(_Config) ->
    catch application:stop(eredis),
    catch application:stop(gproc),
    try
        {ok, _Started} = application:ensure_all_started(eredis),
        ?assertEqual(ok, eredis:stop_sentinel_manager({eredis_SUITE, missing_manager})),
        ?assert(is_pid(whereis(eredis_sup))),
        ?assert(is_pid(whereis(gproc))),
        ?assertEqual(undefined, whereis(eredis_sentinel)),
        ?assertEqual(undefined, whereis(eredis_sentinel_sup)),
        ?assertEqual(undefined, eredis_sentinel_registry:whereis_name({eredis_SUITE, missing_manager}))
    after
        catch application:stop(eredis),
        catch application:stop(gproc)
    end,
    ok.

eredis_sentinel_sup_legacy_start_link_test(_Config) ->
    catch application:stop(eredis),
    catch application:stop(gproc),
    try
        {ok, Sup} = eredis_sentinel_sup:start_link([{"127.0.0.1", 26379}]),
        unlink(Sup),
        Sentinel = whereis(eredis_sentinel),
        ?assertEqual(Sup, whereis(eredis_sentinel_sup)),
        ?assert(is_pid(Sentinel))
    after
        stop_registered_process(eredis_sentinel_sup),
        catch application:stop(eredis),
        catch application:stop(gproc)
    end,
    ok.

eredis_sentinel_sup_replaces_stopped_child_spec_test(_Config) ->
    catch eredis_sentinel:stop(),
    catch application:stop(eredis),
    MasterPort1 = 16381,
    MasterPort2 = 16382,
    {SentinelPort1, SentinelAcceptor1} = start_fake_sentinel(auth_disabled, MasterPort1),
    try
        {ok, Pid1} = eredis_sentinel_sup:start_child([{"127.0.0.1", SentinelPort1}], []),
        ?assertEqual({ok, {"127.0.0.1", MasterPort1}}, eredis_sentinel:get_master(mymaster)),
        ?assertEqual(ok, eredis_sentinel:stop()),
        wait_until_dead(Pid1),
        SentinelAcceptor1 ! stop,
        wait_until_dead(SentinelAcceptor1),

        {SentinelPort2, SentinelAcceptor2} = start_fake_sentinel(auth_disabled, MasterPort2),
        try
            {ok, Pid2} = eredis_sentinel_sup:start_child([{"127.0.0.1", SentinelPort2}], []),
            ?assertNotEqual(Pid1, Pid2),
            ?assertEqual({ok, {"127.0.0.1", MasterPort2}}, eredis_sentinel:get_master(mymaster))
        after
            catch eredis_sentinel:stop(),
            SentinelAcceptor2 ! stop
        end
    after
        catch eredis_sentinel:stop(),
        SentinelAcceptor1 ! stop,
        catch application:stop(eredis)
    end,
    ok.

sentinel_registry_survives_first_starter_exit_test(_Config) ->
    catch application:stop(eredis),
    catch application:stop(gproc),
    Manager1 = {eredis_SUITE, registry_owner_one},
    Manager2 = {eredis_SUITE, registry_owner_two},
    {ok, _Started} = application:ensure_all_started(eredis),
    {Starter1, Pid1} = start_sentinel_manager_from_owner(Manager1),
    {Starter2, Pid2} = start_sentinel_manager_from_owner(Manager2),
    try
        ?assertEqual(Pid1, eredis_sentinel_registry:whereis_name(Manager1)),
        ?assertEqual(Pid2, eredis_sentinel_registry:whereis_name(Manager2)),
        Starter1 ! stop,
        wait_until_dead(Starter1),
        ?assertEqual(Pid2, eredis_sentinel_registry:whereis_name(Manager2)),
        ?assertEqual(
            {ok, {"127.0.0.1", 26379, undefined}},
            eredis_sentinel:get_current_sentinel(Manager2)
        )
    after
        catch eredis_sentinel:stop(Manager1),
        catch eredis_sentinel:stop(Manager2),
        catch exit(Pid1, kill),
        catch exit(Pid2, kill),
        Starter2 ! stop,
        catch application:stop(eredis),
        catch application:stop(gproc)
    end,
    ok.

sentinel_registry_management_api_test(_Config) ->
    catch application:stop(eredis),
    catch application:stop(gproc),
    {ok, _Started} = application:ensure_all_started(eredis),
    Name = {eredis_SUITE, registry_management_api},
    Unknown = {eredis_SUITE, registry_unknown},
    Parent = self(),
    Pid1 = spawn(fun() -> registry_target_loop(Parent) end),
    Pid2 = spawn(fun() -> registry_target_loop(Parent) end),
    try
        ?assertEqual(yes, eredis_sentinel_registry:register_name(Name, Pid1)),
        ?assertEqual(Pid1, eredis_sentinel_registry:whereis_name(Name)),
        ?assertEqual(no, eredis_sentinel_registry:register_name(Name, Pid2)),
        ?assertEqual(Pid1, eredis_sentinel_registry:send(Name, ping)),
        receive
            {Pid1, ping} ->
                ok
        after 1000 ->
            ct:fail(registry_send_timeout)
        end,
        ?assertExit({badarg, {Unknown, ping}}, eredis_sentinel_registry:send(Unknown, ping)),
        ?assertEqual(ok, eredis_sentinel_registry:unregister_name(Name)),
        ?assertEqual(undefined, eredis_sentinel_registry:whereis_name(Name)),
        Owner = eredis_sentinel_registry:whereis_name(Name),
        ?assertEqual(undefined, Owner)
    after
        Pid1 ! stop,
        Pid2 ! stop,
        catch eredis_sentinel_registry:unregister_name(Name),
        catch eredis_sentinel_registry:stop(),
        catch application:stop(eredis),
        catch application:stop(gproc)
    end,
    ok.

sentinel_manager_ref_isolates_sentinel_state_test(_Config) ->
    catch eredis_sentinel:stop(),
    RedisPassword1 = "redis-password-1",
    RedisPassword2 = "redis-password-2",
    {RedisPort1, RedisAcceptor1} = start_fake_redis(RedisPassword1),
    {RedisPort2, RedisAcceptor2} = start_fake_redis(RedisPassword2),
    {SentinelPort1, SentinelAcceptor1} = start_fake_sentinel(auth_disabled, RedisPort1),
    {SentinelPort2, SentinelAcceptor2} = start_fake_sentinel(auth_disabled, RedisPort2),
    try
        {ok, C1} = eredis:start_link(
            sentinel_args_with_manager_ref(
                SentinelPort1,
                RedisPassword1,
                {eredis_SUITE, sentinel_one}
            )
        ),
        try
            ?assertEqual({ok, <<"PONG">>}, eredis:q(C1, ["PING"])),
            {ok, C2} = eredis:start_link(
                sentinel_args_with_manager_ref(
                    SentinelPort2,
                    RedisPassword2,
                    {eredis_SUITE, sentinel_two}
                )
            ),
            try
                ?assertEqual({ok, <<"PONG">>}, eredis:q(C2, ["PING"]))
            after
                eredis:stop(C2)
            end
        after
            eredis:stop(C1)
        end
    after
        cleanup_sentinel_test(RedisAcceptor1, SentinelAcceptor1),
        cleanup_sentinel_test(RedisAcceptor2, SentinelAcceptor2)
    end,
    ok.

sentinel_manager_stop_api_test(_Config) ->
    catch application:stop(eredis),
    catch application:stop(gproc),
    {ok, _Started} = application:ensure_all_started(eredis),
    Ref = {eredis_SUITE, stop_manager_api},
    ManagerName = {eredis_sentinel, Ref},
    catch eredis_sentinel:stop(ManagerName),
    {ok, Pid} = eredis_sentinel:start_link([{"127.0.0.1", 26379}], [], ManagerName),
    try
        ?assertEqual(Pid, eredis_sentinel_registry:whereis_name(ManagerName)),
        ?assertEqual(ok, eredis:stop_sentinel_manager(Ref)),
        wait_until_dead(Pid),
        ?assertEqual(ok, eredis:stop_sentinel_manager(Ref))
    after
        catch eredis_sentinel:stop(ManagerName),
        catch application:stop(eredis),
        catch application:stop(gproc)
    end,
    ok.

start_link_args_without_sentinel_host_port_test(_Config) ->
    {RedisPort, RedisAcceptor} = start_fake_redis(no_auth),
    try
        assert_start_link_args_ping([
            {host, "127.0.0.1"},
            {port, RedisPort},
            {reconnect_sleep, no_reconnect},
            {connect_timeout, 1000}
        ])
    after
        RedisAcceptor ! stop
    end.

start_link_args_without_sentinel_servers_test(_Config) ->
    {RedisPort, RedisAcceptor} = start_fake_redis(no_auth),
    try
        assert_start_link_args_ping([
            {servers, [{"127.0.0.1", RedisPort}]},
            {reconnect_sleep, no_reconnect},
            {connect_timeout, 1000}
        ])
    after
        RedisAcceptor ! stop
    end.

sentinel_credentials_not_inferred_test(_Config) ->
    SharedPassword = "shared-password",
    {RedisPort, RedisAcceptor} = start_fake_redis(SharedPassword),
    {SentinelPort, SentinelAcceptor} = start_fake_sentinel(
        {auth_required, SharedPassword},
        RedisPort
    ),
    try
        assert_sentinel_unreachable_start_failure(
            isolated_start_link(sentinel_args(SentinelPort, SharedPassword, []))
        )
    after
        cleanup_sentinel_test(RedisAcceptor, SentinelAcceptor)
    end,
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
    start_fake_sentinel(Mode, 6379).

start_fake_sentinel(Mode, MasterPort) ->
    {ok, Listen} = gen_tcp:listen(0, [binary, {active, false}, {packet, raw}, {ip, {127, 0, 0, 1}}]),
    {ok, Port} = inet:port(Listen),
    Mode1 = normalize_sentinel_mode(Mode),
    Acceptor = spawn_link(fun() -> fake_sentinel_accept(Listen, Mode1, MasterPort) end),
    {Port, Acceptor}.

normalize_sentinel_mode({auth_required, Password}) ->
    {auth_required, iolist_to_binary(Password)};
normalize_sentinel_mode({auth_required, Username, Password}) ->
    {auth_required, iolist_to_binary(Username), iolist_to_binary(Password)};
normalize_sentinel_mode(Mode) ->
    Mode.

fake_sentinel_accept(Listen, Mode, MasterPort) ->
    receive
        stop ->
            gen_tcp:close(Listen)
    after 0 ->
        case gen_tcp:accept(Listen, 100) of
            {ok, Socket} ->
                case fake_sentinel_loop(
                    Listen,
                    Socket,
                    Mode,
                    MasterPort,
                    initial_auth_state(Mode),
                    <<>>
                ) of
                    stopped -> ok;
                    ok -> fake_sentinel_accept(Listen, Mode, MasterPort)
                end;
            {error, timeout} ->
                fake_sentinel_accept(Listen, Mode, MasterPort)
        end
    end.

initial_auth_state(auth_required) ->
    false;
initial_auth_state({auth_required, _Password}) ->
    false;
initial_auth_state({auth_required, _Username, _Password}) ->
    false;
initial_auth_state(_) ->
    true.

fake_sentinel_loop(Listen, Socket, Mode, MasterPort, Authed, Buffer) ->
    receive
        stop ->
            gen_tcp:close(Socket),
            gen_tcp:close(Listen),
            stopped
    after 0 ->
        case gen_tcp:recv(Socket, 0, 100) of
            {ok, Data} ->
                handle_fake_sentinel_data(
                    Listen,
                    Socket,
                    Mode,
                    MasterPort,
                    Authed,
                    <<Buffer/binary, Data/binary>>
                );
            {error, timeout} ->
                fake_sentinel_loop(Listen, Socket, Mode, MasterPort, Authed, Buffer);
            {error, closed} ->
                ok
        end
    end.

handle_fake_sentinel_data(Listen, Socket, Mode, MasterPort, Authed, Buffer) ->
    case parse_resp_commands(Buffer) of
        {ok, Commands, Rest} ->
            case handle_fake_sentinel_commands(Socket, Mode, MasterPort, Authed, Commands) of
                {continue, Authed1} ->
                    fake_sentinel_loop(Listen, Socket, Mode, MasterPort, Authed1, Rest);
                close ->
                    gen_tcp:close(Socket)
            end;
        more ->
            fake_sentinel_loop(Listen, Socket, Mode, MasterPort, Authed, Buffer)
    end.

handle_fake_sentinel_commands(_Socket, _Mode, _MasterPort, Authed, []) ->
    {continue, Authed};
handle_fake_sentinel_commands(Socket, Mode, MasterPort, Authed, [Command | Rest]) ->
    case handle_fake_sentinel_command(Socket, Mode, MasterPort, Authed, Command) of
        {continue, Authed1} ->
            handle_fake_sentinel_commands(Socket, Mode, MasterPort, Authed1, Rest);
        close ->
            close
    end.

handle_fake_sentinel_command(Socket, ping_forbidden, _MasterPort, Authed, [<<"PING">>]) ->
    ok = gen_tcp:send(Socket, <<"-ERR PING should not be sent.\r\n">>),
    {continue, Authed};
handle_fake_sentinel_command(_Socket, ping_timeout, _MasterPort, Authed, [<<"PING">>]) ->
    {continue, Authed};
handle_fake_sentinel_command(Socket, _Mode, _MasterPort, true, [<<"PING">>]) ->
    ok = gen_tcp:send(Socket, <<"+PONG\r\n">>),
    {continue, true};
handle_fake_sentinel_command(Socket, auth_required, _MasterPort, _Authed, [<<"AUTH">>, <<"public">>]) ->
    ok = gen_tcp:send(Socket, <<"+OK\r\n">>),
    {continue, true};
handle_fake_sentinel_command(Socket, {auth_required, Password}, _MasterPort, _Authed, [<<"AUTH">>, Password]) ->
    ok = gen_tcp:send(Socket, <<"+OK\r\n">>),
    {continue, true};
handle_fake_sentinel_command(Socket, {auth_required, Username, Password}, _MasterPort, _Authed, [<<"AUTH">>, Username, Password]) ->
    ok = gen_tcp:send(Socket, <<"+OK\r\n">>),
    {continue, true};
handle_fake_sentinel_command(Socket, auth_required, _MasterPort, _Authed, [<<"AUTH">> | _]) ->
    ok = gen_tcp:send(Socket, <<"-WRONGPASS invalid username-password pair or user is disabled.\r\n">>),
    close;
handle_fake_sentinel_command(Socket, {auth_required, _Password}, _MasterPort, _Authed, [<<"AUTH">> | _]) ->
    ok = gen_tcp:send(Socket, <<"-WRONGPASS invalid username-password pair or user is disabled.\r\n">>),
    close;
handle_fake_sentinel_command(Socket, {auth_required, _Username, _Password}, _MasterPort, _Authed, [<<"AUTH">> | _]) ->
    ok = gen_tcp:send(Socket, <<"-WRONGPASS invalid username-password pair or user is disabled.\r\n">>),
    close;
handle_fake_sentinel_command(Socket, _Mode, _MasterPort, _Authed, [<<"AUTH">>, <<"public">>]) ->
    ok = gen_tcp:send(Socket, <<"-ERR AUTH <password> called without any password configured for the default user.\r\n">>),
    close;
handle_fake_sentinel_command(Socket, _Mode, MasterPort, true, [<<"SENTINEL">>, <<"get-master-addr-by-name">>, <<"mymaster">>]) ->
    PortBin = integer_to_binary(MasterPort),
    PortSize = integer_to_binary(byte_size(PortBin)),
    ok = gen_tcp:send(Socket, [
        <<"*2\r\n$9\r\n127.0.0.1\r\n$">>,
        PortSize,
        <<"\r\n">>,
        PortBin,
        <<"\r\n">>
    ]),
    {continue, true};
handle_fake_sentinel_command(Socket, _Mode, _MasterPort, true, _Command) ->
    ok = gen_tcp:send(Socket, <<"-ERR unknown command\r\n">>),
    {continue, true};
handle_fake_sentinel_command(Socket, _Mode, _MasterPort, Authed, _Command) ->
    ok = gen_tcp:send(Socket, <<"-NOAUTH Authentication required.\r\n">>),
    {continue, Authed}.

sentinel_args(SentinelPort, ExtraArgs) ->
    [
        {servers, [{"127.0.0.1", SentinelPort}]},
        {options, [{sentinel, "mymaster"}]},
        {reconnect_sleep, no_reconnect},
        {connect_timeout, 1000}
    ] ++ ExtraArgs.

sentinel_args(SentinelPort, RedisPassword, ExtraArgs) ->
    sentinel_args(SentinelPort, [{password, RedisPassword} | ExtraArgs]).

sentinel_args_with_manager_ref(SentinelPort, RedisPassword, ManagerRef) ->
    [
        {servers, [{"127.0.0.1", SentinelPort}]},
        {options, [{sentinel, "mymaster"}, {sentinel_manager_ref, ManagerRef}]},
        {password, RedisPassword},
        {reconnect_sleep, no_reconnect},
        {connect_timeout, 1000}
    ].

start_sentinel_manager_from_owner(ManagerName) ->
    Parent = self(),
    Starter = spawn(fun() ->
        Result = eredis_sentinel:start_link([{"127.0.0.1", 26379}], [], ManagerName),
        Parent ! {self(), Result},
        receive
            stop ->
                ok
        end
    end),
    receive
        {Starter, {ok, Pid}} ->
            {Starter, Pid};
        {Starter, Error} ->
            ct:fail({failed_to_start_sentinel_manager, Error})
    after 1000 ->
        ct:fail(sentinel_manager_start_timeout)
    end.

wait_until_dead(Pid) ->
    wait_until_dead(Pid, 10).

wait_until_dead(Pid, 0) ->
    ?assertNot(is_process_alive(Pid));
wait_until_dead(Pid, Tries) ->
    case is_process_alive(Pid) of
        true ->
            timer:sleep(10),
            wait_until_dead(Pid, Tries - 1);
        false ->
            ok
    end.

stop_registered_process(Name) ->
    case whereis(Name) of
        undefined ->
            ok;
        Pid ->
            exit(Pid, kill),
            wait_until_dead(Pid)
    end.

registry_target_loop(Parent) ->
    receive
        stop ->
            ok;
        Msg ->
            Parent ! {self(), Msg},
            registry_target_loop(Parent)
    end.

assert_start_link_args_ping(Args) ->
    case eredis:start_link(Args) of
        {ok, C} ->
            try
                ?assertEqual({ok, <<"PONG">>}, eredis:q(C, ["PING"]))
            after
                eredis:stop(C)
            end;
        Error ->
            ct:fail({start_link_failed, Error})
    end.

assert_sentinel_connection(
    #{name := Name, redis := RedisMode, sentinel := SentinelMode, args := Args}
) ->
    {RedisPort, RedisAcceptor} = start_fake_redis(RedisMode),
    {SentinelPort, SentinelAcceptor} = start_fake_sentinel(SentinelMode, RedisPort),
    try
        C = start_sentinel_connection(Name, SentinelPort, Args),
        try
            ?assertEqual({Name, {ok, <<"PONG">>}}, {Name, eredis:q(C, ["PING"])})
        after
            eredis:stop(C)
        end
    after
        cleanup_sentinel_test(RedisAcceptor, SentinelAcceptor)
    end.

start_sentinel_connection(Name, SentinelPort, Args) ->
    case catch eredis:start_link(sentinel_args(SentinelPort, Args)) of
        {ok, C} ->
            C;
        Error ->
            ct:fail({Name, Error})
    end.

assert_sentinel_unreachable_start_failure({error, {sentinel_error, sentinel_unreachable}}) ->
    ok;
assert_sentinel_unreachable_start_failure({'EXIT', {sentinel_error, sentinel_unreachable}}) ->
    ok.

isolated_start_link(Args) ->
    Parent = self(),
    Ref = make_ref(),
    {Pid, Mon} = spawn_monitor(fun() ->
        process_flag(trap_exit, true),
        Result = catch eredis:start_link(Args),
        flush_sentinel_unreachable_exit(),
        Parent ! {Ref, Result}
    end),
    receive
        {Ref, Result} ->
            wait_for_worker_down(Mon, Pid),
            Result;
        {'DOWN', Mon, process, Pid, normal} ->
            receive
                {Ref, Result} ->
                    Result
            after 1000 ->
                {'EXIT', normal}
            end;
        {'DOWN', Mon, process, Pid, Reason} ->
            {'EXIT', Reason}
    end.

wait_for_worker_down(Mon, Pid) ->
    receive
        {'DOWN', Mon, process, Pid, _Reason} ->
            ok
    after 1000 ->
        ok
    end.

flush_sentinel_unreachable_exit() ->
    receive
        {'EXIT', _Pid, {sentinel_error, sentinel_unreachable}} ->
            ok
    after 100 ->
        ok
    end.

cleanup_sentinel_test(RedisAcceptor, SentinelAcceptor) ->
    catch eredis_sentinel:stop(),
    RedisAcceptor ! stop,
    SentinelAcceptor ! stop.

start_fake_redis(Mode) ->
    {ok, Listen} = gen_tcp:listen(0, [binary, {active, false}, {packet, raw}, {ip, {127, 0, 0, 1}}]),
    {ok, Port} = inet:port(Listen),
    Mode1 = normalize_redis_mode(Mode),
    Acceptor = spawn_link(fun() -> fake_redis_accept(Listen, Mode1) end),
    {Port, Acceptor}.

normalize_redis_mode(no_auth) ->
    no_auth;
normalize_redis_mode(Password) ->
    {auth_required, iolist_to_binary(Password)}.

redis_initial_auth_state(no_auth) ->
    true;
redis_initial_auth_state({auth_required, _Password}) ->
    false.

fake_redis_accept(Listen, Mode) ->
    receive
        stop ->
            gen_tcp:close(Listen)
    after 0 ->
        case gen_tcp:accept(Listen, 100) of
            {ok, Socket} ->
                case fake_redis_loop(
                    Listen,
                    Socket,
                    Mode,
                    redis_initial_auth_state(Mode),
                    <<>>
                ) of
                    stopped -> ok;
                    ok -> fake_redis_accept(Listen, Mode)
                end;
            {error, timeout} ->
                fake_redis_accept(Listen, Mode)
        end
    end.

fake_redis_loop(Listen, Socket, Mode, Authed, Buffer) ->
    receive
        stop ->
            gen_tcp:close(Socket),
            gen_tcp:close(Listen),
            stopped
    after 0 ->
        case gen_tcp:recv(Socket, 0, 100) of
            {ok, Data} ->
                handle_fake_redis_data(Listen, Socket, Mode, Authed, <<Buffer/binary, Data/binary>>);
            {error, timeout} ->
                fake_redis_loop(Listen, Socket, Mode, Authed, Buffer);
            {error, closed} ->
                ok
        end
    end.

handle_fake_redis_data(Listen, Socket, Mode, Authed, Buffer) ->
    case parse_resp_commands(Buffer) of
        {ok, Commands, Rest} ->
            case handle_fake_redis_commands(Socket, Mode, Authed, Commands) of
                {continue, Authed1} ->
                    fake_redis_loop(Listen, Socket, Mode, Authed1, Rest);
                close ->
                    gen_tcp:close(Socket)
            end;
        more ->
            fake_redis_loop(Listen, Socket, Mode, Authed, Buffer)
    end.

handle_fake_redis_commands(_Socket, _Mode, Authed, []) ->
    {continue, Authed};
handle_fake_redis_commands(Socket, Mode, Authed, [Command | Rest]) ->
    case handle_fake_redis_command(Socket, Mode, Authed, Command) of
        {continue, Authed1} ->
            handle_fake_redis_commands(Socket, Mode, Authed1, Rest);
        close ->
            close
    end.

handle_fake_redis_command(Socket, no_auth, _Authed, [<<"AUTH">> | _]) ->
    ok = gen_tcp:send(Socket, <<"-ERR AUTH <password> called without any password configured for the default user.\r\n">>),
    close;
handle_fake_redis_command(Socket, {auth_required, Password}, _Authed, [<<"AUTH">>, Password]) ->
    ok = gen_tcp:send(Socket, <<"+OK\r\n">>),
    {continue, true};
handle_fake_redis_command(Socket, {auth_required, _Password}, _Authed, [<<"AUTH">> | _]) ->
    ok = gen_tcp:send(Socket, <<"-WRONGPASS invalid username-password pair or user is disabled.\r\n">>),
    close;
handle_fake_redis_command(Socket, _Mode, true, [<<"PING">>]) ->
    ok = gen_tcp:send(Socket, <<"+PONG\r\n">>),
    {continue, true};
handle_fake_redis_command(Socket, _Mode, true, [<<"SELECT">>, _Database]) ->
    ok = gen_tcp:send(Socket, <<"+OK\r\n">>),
    {continue, true};
handle_fake_redis_command(Socket, _Mode, true, _Command) ->
    ok = gen_tcp:send(Socket, <<"-ERR unknown command\r\n">>),
    {continue, true};
handle_fake_redis_command(Socket, _Mode, Authed, _Command) ->
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
