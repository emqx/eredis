-module(eredis_sup).

-behaviour(supervisor).

-export([start_link/0, ensure_sentinel_sup/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

ensure_sentinel_sup() ->
    ok = ensure_application_started(),
    ensure_sentinel_supervisor_started().

init([]) ->
    %% Keep Sentinel resources lazy: most eredis users do not use Sentinel,
    %% so the root application starts with no Sentinel children.
    {ok, {{one_for_one, 10, 100}, []}}.

ensure_sentinel_supervisor_started() ->
    case whereis(eredis_sentinel_sup) of
        undefined ->
            start_sentinel_sup();
        Pid when is_pid(Pid) ->
            ok
    end.

ensure_application_started() ->
    case application:ensure_all_started(eredis) of
        {ok, _Started} ->
            ok;
        {error, {already_started, eredis}} ->
            ok;
        {error, Reason} ->
            exit({eredis_application_start_failed, Reason})
    end.

start_sentinel_sup() ->
    ChildSpec = sentinel_sup_child_spec(),
    case supervisor:start_child(?MODULE, ChildSpec) of
        {ok, _Pid} ->
            ok;
        {ok, _Pid, _Info} ->
            ok;
        {error, {already_started, _Pid}} ->
            ok;
        {error, already_present} ->
            replace_stopped_child(ChildSpec);
        {error, Reason} ->
            exit({sentinel_supervisor_start_failed, Reason})
    end.

replace_stopped_child(ChildSpec) ->
    ChildId = maps:get(id, ChildSpec),
    case supervisor:delete_child(?MODULE, ChildId) of
        ok ->
            start_sentinel_sup();
        {error, running} ->
            ok;
        {error, not_found} ->
            start_sentinel_sup();
        {error, Reason} ->
            exit({sentinel_supervisor_replace_failed, Reason})
    end.

sentinel_sup_child_spec() ->
    #{
        id => eredis_sentinel_sup,
        start => {eredis_sentinel_sup, start_link, []},
        restart => transient,
        shutdown => infinity,
        type => supervisor,
        modules => [eredis_sentinel_sup]
    }.
