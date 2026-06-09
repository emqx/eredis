%% Local registry for named Sentinel managers.
%%
%% This module intentionally keeps gproc behind the standard `via' registry
%% callbacks. The rest of eredis should not depend on the gproc key shape or on
%% direct gproc calls. Sentinel is optional, so the Sentinel supervisor and
%% manager processes are still started only when Sentinel mode is used.
-module(eredis_sentinel_registry).

-export([register_name/2, unregister_name/1, whereis_name/1, send/2, stop/0]).

register_name(Name, Pid) when is_pid(Pid) ->
    case whereis_name(Name) of
        undefined ->
            try
                true = register_gproc_name(key(Name), Pid),
                yes
            catch
                error:_ ->
                    no
            end;
        _Pid ->
            no
    end.

unregister_name(Name) ->
    case whereis_name(Name) of
        undefined ->
            ok;
        Pid when Pid =:= self() ->
            catch gproc:unregister_name(key(Name)),
            ok;
        Pid ->
            catch gproc:unreg_other(key(Name), Pid),
            ok
    end.

whereis_name(Name) ->
    gproc:whereis_name(key(Name)).

send(Name, Msg) ->
    case whereis_name(Name) of
        undefined ->
            exit({badarg, {Name, Msg}});
        Pid ->
            Pid ! Msg,
            Pid
    end.

stop() ->
    ok.

key(Name) ->
    {n, l, {?MODULE, Name}}.

register_gproc_name(Key, Pid) when Pid =:= self() ->
    yes = gproc:register_name(Key, Pid),
    true;
register_gproc_name(Key, Pid) ->
    gproc:reg_other(Key, Pid).
