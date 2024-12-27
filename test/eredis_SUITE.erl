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
        {group, ssl}
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
