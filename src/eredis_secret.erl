%% Note: this module CAN'T be hot-patched to avoid invalidating the
%% closures, so it must not be changed.
-module(eredist_secret).

%% API:
-export([wrap/1, unwrap/1]).

%%================================================================================
%% API funcions
%%================================================================================

wrap(undefined) -> undefined;
wrap(Func) when is_function(Func, 0) ->
    Func;
wrap(Term) ->
    fun() ->
        Term
    end.

unwrap(Term) when is_function(Term, 0) ->
    %% Handle potentially nested funs
    unwrap(Term());
unwrap(Term) ->
    Term.
