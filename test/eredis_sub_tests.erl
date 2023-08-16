-module(eredis_sub_tests).

-include_lib("eunit/include/eunit.hrl").
-include("eredis.hrl").
-include("eredis_sub.hrl").

-define(AUTH_PASS_ONLY_PASSWORD, "public").
-define(AUTH_USER_PASS_USERNAME, "test_user").
-define(AUTH_USER_PASS_PASSWORD, "test_passwd").

-import(eredis, [create_multibulk/1]).

maybe_credential(password_only) ->
    ?AUTH_PASS_ONLY_PASSWORD;
maybe_credential(username_password) ->
    eredis:make_credentials(?AUTH_USER_PASS_USERNAME, ?AUTH_USER_PASS_PASSWORD).

c(AuthMethod) ->
    Res = eredis:start_link("127.0.0.1", 6379, 0, maybe_credential(AuthMethod)),
    ?assertMatch({ok, _}, Res),
    {ok, C} = Res,
    C.

s(AuthMethod) ->
    Res = eredis_sub:start_link("127.0.0.1", 6379, maybe_credential(AuthMethod), 0),
    ?assertMatch({ok, _}, Res),
    {ok, C} = Res,
    C.

add_channels(Sub, Channels) ->
    ok = eredis_sub:controlling_process(Sub),
    ok = eredis_sub:subscribe(Sub, Channels),
    lists:foreach(
      fun (C) ->
              receive M ->
                      ?assertEqual({subscribed, C, Sub}, M),
                      eredis_sub:ack_message(Sub)
              end
      end, Channels).

basic_test_() ->
    [
        basic_test_cases(password_only),
        basic_test_cases(username_password)
    ].

basic_test_cases(AuthMethod) ->
    AuthMethodSuffix =
        case AuthMethod of
            password_only -> " - password only";
            username_password -> " - username/password"
        end,
    [
        { "pubsub" ++ AuthMethodSuffix,
            fun() ->
                Pub = c(AuthMethod),
                Sub = s(AuthMethod),
                add_channels(Sub, [<<"chan1">>, <<"chan2">>]),
                ok = eredis_sub:controlling_process(Sub),

                ?assertEqual({ok, <<"1">>}, eredis:q(Pub, ["PUBLISH", chan1, msg])),
                receive
                    {message, _, _, _} = M ->
                        ?assertEqual({message, <<"chan1">>, <<"msg">>, Sub}, M)
                after 10 ->
                        throw(timeout)
                end,

                receive
                    Msg ->
                        throw({unexpected_message, Msg})
                after 5 ->
                        ok
                end,
                eredis_sub:stop(Sub)
            end
        },

        %% Push size so high, the queue will be used
        { "pubsub2" ++ AuthMethodSuffix,
            fun() ->
                Pub = c(AuthMethod),
                Sub = s(AuthMethod),
                add_channels(Sub, [<<"chan">>]),
                ok = eredis_sub:controlling_process(Sub),
                lists:foreach(
                fun(_) ->
                        Msg = binary:copy(<<"0">>, 2048),
                        ?assertEqual({ok, <<"1">>}, eredis:q(Pub, [publish, chan, Msg]))
                end, lists:seq(1, 500)),
                Msgs = recv_all(Sub),
                ?assertEqual(500, length(Msgs)),
                eredis_sub:stop(Sub)
            end
        },

        { "pubsub manage subscribers" ++ AuthMethodSuffix,
            fun() ->
                Pub = c(AuthMethod),
                Sub = s(AuthMethod),
                add_channels(Sub, [<<"chan">>]),
                unlink(Sub),
                Self = self(),
                ?assertMatch(#state{controlling_process={_, Self}}, get_state(Sub)),
                S1 = subscriber(Sub),
                ok = eredis_sub:controlling_process(Sub, S1),
                #state{controlling_process={_, S1}} = get_state(Sub),
                S2 = subscriber(Sub),
                ok = eredis_sub:controlling_process(Sub, S2),
                #state{controlling_process={_, S2}} = get_state(Sub),
                eredis:q(Pub, ["PUBLISH", chan, msg1]),
                S1 ! stop,
                ok = wait_for_stop(S1),
                eredis:q(Pub, ["PUBLISH", chan, msg2]),
                ?assertEqual({message, <<"chan">>, <<"msg1">>, Sub}, wait_for_msg(S2)),
                ?assertEqual({message, <<"chan">>, <<"msg2">>, Sub}, wait_for_msg(S2)),
                S2 ! stop,
                ok = wait_for_stop(S2),
                Ref = erlang:monitor(process, Sub),
                receive {'DOWN', Ref, process, Sub, _} -> ok end
            end
        },

        { "pubsub connect/disconnect messages" ++ AuthMethodSuffix,
            fun() ->
                Pub = c(AuthMethod),
                Sub = s(AuthMethod),
                add_channels(Sub, [<<"chan">>]),
                S = subscriber(Sub),
                ok = eredis_sub:controlling_process(Sub, S),
                eredis:q(Pub, ["PUBLISH", chan, msg]),
                wait_for_msg(S),
                #state{socket=Sock} = get_state(Sub),
                gen_tcp:close(Sock),
                Sub ! {tcp_closed, Sock},
                ?assertEqual({eredis_disconnected, Sub}, wait_for_msg(S)),
                ?assertEqual({eredis_reconnect_attempt, Sub}, wait_for_msg(S)),
                ?assertEqual({eredis_connected, Sub}, wait_for_msg(S)),
                eredis_sub:stop(Sub)
            end
        },

        { "drop queue" ++ AuthMethodSuffix,
            fun() ->
                Pub = c(AuthMethod),
                MaybeCredential = maybe_credential(AuthMethod),
                {ok, Sub} = eredis_sub:start_link("127.0.0.1", 6379, MaybeCredential, 0, 100, 10, drop),
                add_channels(Sub, [<<"foo">>]),
                ok = eredis_sub:controlling_process(Sub),

                [eredis:q(Pub, [publish, foo, N]) || N <- lists:seq(1, 12)],

                receive M1 -> ?assertEqual({message,<<"foo">>,<<"1">>, Sub}, M1) end,
                receive M2 -> ?assertEqual({dropped, 11}, M2) end,
                eredis_sub:stop(Sub)
            end
        },

        { "crash queue" ++ AuthMethodSuffix,
            fun() ->
                Pub = c(AuthMethod),
                MaybeCredential = maybe_credential(AuthMethod),
                {ok, Sub} = eredis_sub:start_link("127.0.0.1", 6379, MaybeCredential, 0, 100, 10, exit),
                add_channels(Sub, [<<"foo">>]),

                true = unlink(Sub),
                ok = eredis_sub:controlling_process(Sub),
                Ref = erlang:monitor(process, Sub),

                [eredis:q(Pub, [publish, foo, N]) || N <- lists:seq(1, 12)],

                receive M1 -> ?assertEqual({message,<<"foo">>,<<"1">>, Sub}, M1) end,
                receive M2 -> ?assertEqual({'DOWN', Ref, process, Sub, max_queue_size}, M2) end
            end
        },

        { "dynamic channels" ++ AuthMethodSuffix,
            fun() ->
                Pub = c(AuthMethod),
                Sub = s(AuthMethod),
                ok = eredis_sub:controlling_process(Sub),

                eredis:q(Pub, [publish, newchan, foo]),

                receive {message, <<"foo">>, _, _} -> ?assert(false)
                after 5 -> ok end,

                %% We do the following twice to show that subscribing to the same channel
                %% doesn't cause the channel to show up twice
                lists:foreach(fun(_) ->
                    eredis_sub:subscribe(Sub, [<<"newchan">>, <<"otherchan">>]),
                    receive M1 -> ?assertEqual({subscribed, <<"newchan">>, Sub}, M1) end,
                    eredis_sub:ack_message(Sub),
                    receive M2 -> ?assertEqual({subscribed, <<"otherchan">>, Sub}, M2) end,
                    eredis_sub:ack_message(Sub),

                    {ok, Channels} = eredis_sub:channels(Sub),
                    ?assertEqual(true, lists:member(<<"otherchan">>, Channels)),
                    ?assertEqual(true, lists:member(<<"newchan">>, Channels)),
                    ?assertEqual(2, length(Channels))
                end, lists:seq(0, 1)),

                eredis:q(Pub, [publish, newchan, foo]),
                ?assertEqual([{message, <<"newchan">>, <<"foo">>, Sub}], recv_all(Sub)),
                eredis:q(Pub, [publish, otherchan, foo]),
                ?assertEqual([{message, <<"otherchan">>, <<"foo">>, Sub}], recv_all(Sub)),

                eredis_sub:unsubscribe(Sub, [<<"otherchan">>]),
                eredis_sub:ack_message(Sub),
                receive M3 -> ?assertEqual({unsubscribed, <<"otherchan">>, Sub}, M3) end,
                ?assertEqual({ok, [<<"newchan">>]}, eredis_sub:channels(Sub))
            end
        },

        { "censored password" ++ AuthMethodSuffix,
            fun() ->
                Sub = s(AuthMethod),
                #state{credentials = Credentials} = get_state(Sub),
                ?assertMatch(#{password := "******"}, eredis:get_credentials_info(Credentials))
            end
        }
    ].

recv_all(Sub) ->
    recv_all(Sub, []).

recv_all(Sub, Acc) ->
    receive
        {message, _, _, _} = InMsg ->
            eredis_sub:ack_message(Sub),
            recv_all(Sub, [InMsg | Acc])
    after 5 ->
              lists:reverse(Acc)
    end.

subscriber(Client) ->
    Test = self(),
    Pid = spawn(fun () -> subscriber(Client, Test) end),
    spawn(fun() ->
                  Ref = erlang:monitor(process, Pid),
                  receive
                      {'DOWN', Ref, _, _, _} ->
                          Test ! {stopped, Pid}
                  end
          end),
    Pid.

subscriber(Client, Test) ->
    receive
        stop ->
            ok;
        Msg ->
            Test ! {got_message, self(), Msg},
            eredis_sub:ack_message(Client),
            subscriber(Client, Test)
    end.

wait_for_msg(Subscriber) ->
    receive
        {got_message, Subscriber, Msg} ->
            Msg
    end.

wait_for_stop(Subscriber) ->
    receive
        {stopped, Subscriber} ->
            ok
    end.

get_state(Pid)
  when is_pid(Pid) ->
    {status, _, _, [_, _, _, _, State]} = sys:get_status(Pid),
    get_state(State);
get_state([{data, [{"State", State}]} | _]) ->
    State;
get_state([_|Rest]) ->
    get_state(Rest).

% Tests for Pattern Subscribe
add_channels_pattern(Sub, Channels) ->
    ok = eredis_sub:controlling_process(Sub),
    ok = eredis_sub:psubscribe(Sub, Channels),
    lists:foreach(
      fun (C) ->
              receive M ->
                      ?assertEqual({subscribed, C, Sub}, M),
                      eredis_sub:ack_message(Sub)
              end
      end, Channels).

pubsub_pattern_test_() ->
    [
        pubsub_pattern_test_cases(password_only),
        pubsub_pattern_test_cases(username_password)
    ].

pubsub_pattern_test_cases(AuthMethod) ->
    AuthMethodSuffix =
        case AuthMethod of
            password_only -> " - password only";
            username_password -> " - username/password"
        end,
    [
        { "pubsub_pattern_test" ++ AuthMethodSuffix,
            fun() ->
                Pub = c(AuthMethod),
                Sub = s(AuthMethod),
                add_channels_pattern(Sub, [<<"chan1*">>, <<"chan2*">>]),
                ok = eredis_sub:controlling_process(Sub),

                ?assertEqual({ok, <<"1">>}, eredis:q(Pub, ["PUBLISH", <<"chan123">>, <<"msg">>])),
                receive
                    {pmessage, _Pattern, _Channel, _Message, _} = M ->
                        ?assertEqual({pmessage, <<"chan1*">>,<<"chan123">>, <<"msg">>, Sub}, M)
                after 10 ->
                        throw(timeout)
                end,

                eredis_sub:punsubscribe(Sub, [<<"chan1*">> , <<"chan2*">>]),
                eredis_sub:ack_message(Sub),
                eredis_sub:ack_message(Sub),
                receive {unsubscribed,_,_} = M2 -> ?assertEqual({unsubscribed, <<"chan1*">>, Sub}, M2) end,
                eredis_sub:ack_message(Sub),
                receive {unsubscribed,_,_} =  M3 -> ?assertEqual({unsubscribed, <<"chan2*">>, Sub}, M3) end,
                eredis_sub:ack_message(Sub),

                ?assertEqual({ok, <<"0">>}, eredis:q(Pub, ["PUBLISH", <<"chan123">>, <<"msg">>])),
                receive
                    Msg -> throw({unexpected_message, Msg})
                after 10 ->
                        ok
                end,

                eredis_sub:stop(Sub)
            end
        }
    ].

