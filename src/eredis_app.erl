-module(eredis_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    eredis_sup:start_link().

stop(_State) ->
    ok.
