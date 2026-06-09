-module(eredis_sentinel_sup).

-behaviour(supervisor).

-export([ start_link/0
        , start_link/1
        , start_child/1
        , start_child/2
        , start_child/3
        ]).

-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_link(Env) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, [Env]).

start_child(Env) ->
    start_child_spec(child_spec(Env)).

start_child(Sentinels, Opts) ->
    start_child_spec(child_spec(Sentinels, Opts)).

start_child(Sentinels, Opts, ManagerName) ->
    start_child_spec(child_spec(Sentinels, Opts, ManagerName)).

init([]) ->
    {ok, {{one_for_one, 10, 100}, []}};

init([Env]) ->
    {ok, {{one_for_one, 10, 100}, [legacy_child_spec(Env)]}}.

start_child_spec(ChildSpec) ->
    ok = ensure_supervisor_started(),
    case supervisor:start_child(?MODULE, ChildSpec) of
        {ok, Pid} ->
            {ok, Pid};
        {ok, Pid, _Info} ->
            {ok, Pid};
        {error, {already_started, Pid}} ->
            {ok, Pid};
        {error, {{already_started, Pid}, _}} ->
            {ok, Pid};
        {error, already_present} ->
            replace_stopped_child(ChildSpec);
        {error, Error} ->
            {error, Error}
    end.

ensure_supervisor_started() ->
    eredis_sup:ensure_sentinel_sup().

replace_stopped_child(ChildSpec) ->
    ChildId = maps:get(id, ChildSpec),
    case supervisor:delete_child(?MODULE, ChildId) of
        ok ->
            start_child_spec(ChildSpec);
        {error, running} ->
            find_child_pid(ChildId);
        {error, not_found} ->
            start_child_spec(ChildSpec);
        {error, Error} ->
            {error, Error}
    end.

find_child_pid(ChildId) ->
    case lists:keyfind(ChildId, 1, supervisor:which_children(?MODULE)) of
        {ChildId, Pid, _Type, _Modules} when is_pid(Pid) ->
            {ok, Pid};
        false ->
            {error, not_found};
        _Child ->
            {error, not_running}
    end.

legacy_child_spec(Env) ->
    (child_spec(Env))#{restart := permanent}.

child_spec(Env) ->
    #{
        id => eredis_sentinel,
        start => {eredis_sentinel, start_link, [Env]},
        restart => transient,
        shutdown => 5000,
        type => worker,
        modules => [eredis_sentinel]
    }.

child_spec(Sentinels, Opts) ->
    #{
        id => eredis_sentinel,
        start => {eredis_sentinel, start_link, [Sentinels, Opts]},
        restart => transient,
        shutdown => 5000,
        type => worker,
        modules => [eredis_sentinel]
    }.

child_spec(Sentinels, Opts, ManagerName) ->
    #{
        id => {eredis_sentinel, ManagerName},
        start => {eredis_sentinel, start_link, [Sentinels, Opts, ManagerName]},
        restart => transient,
        shutdown => 5000,
        type => worker,
        modules => [eredis_sentinel]
    }.
