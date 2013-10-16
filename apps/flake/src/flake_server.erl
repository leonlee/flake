%%%
%%% Copyright 2012, Boundary
%%%
%%% Licensed under the Apache License, Version 2.0 (the "License");
%%% you may not use this file except in compliance with the License.
%%% You may obtain a copy of the License at
%%%
%%%     http://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing, software
%%% distributed under the License is distributed on an "AS IS" BASIS,
%%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%% See the License for the specific language governing permissions and
%%% limitations under the License.
%%%

-module(flake_server).
-author('Dietrich Featherston <d@boundary.com>').

-behaviour(gen_server).

%% API
-export([
    start_link/1,
    id/0,
    id/1,
    id_in/2, id_in/1]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-include_lib("eunit/include/eunit.hrl").

-record(state, {max_time, worker_id, sequence}).

%% ----------------------------------------------------------
%% API
%% ----------------------------------------------------------

% start and link to a new flake id generator
start_link(Config) ->
    Name = xor_pl:read(name, Config),
    gen_server:start_link({local, Name}, ?MODULE, Config, []).

% generate a new snowflake id
id() ->
    id(0).
id(ZoneId) ->
    respond(gen_server:call(flake, {get, ZoneId})).

id_in(Base) ->
    id_in(0, Base).
id_in(ZoneId, Base) ->
    respond(gen_server:call(flake, {get, ZoneId, Base})).

respond({ok, Flake}) ->
    {ok, Flake};
respond(X) ->
    X.

%% ----------------------------------------------------------
%% gen_server callbacks
%% ----------------------------------------------------------

init(Config) ->
    WorkerId = xor_pl:read(worker_id, Config),
    {ok, #state{max_time = flake_util:curr_time_millis(), worker_id = WorkerId, sequence = 0}}.

handle_call({get, ZoneId}, _From, State = #state{max_time = MaxTime, worker_id = WorkerId, sequence = Sequence}) ->
    {Resp, S0} = get(flake_util:curr_time_millis(), MaxTime, ZoneId, WorkerId, Sequence, State),
    {reply, Resp, S0};

handle_call({get, ZoneId, Base}, _From,
            State = #state{max_time = MaxTime, worker_id = WorkerId, sequence = Sequence}) ->
    {Resp, S0} = get(flake_util:curr_time_millis(), MaxTime, ZoneId, WorkerId, Sequence, State),
    case Resp of
        {ok, Id} ->
            {reply, {ok, xor_util:to_base(Id, Base)}, S0};
        E ->
            {reply, E, S0}
    end;

handle_call(X, _From, State) ->
    error_logger:error_msg("unrecognized msg in ~p:handle_call -> ~p~n", [?MODULE, X]),
    {reply, ok, State}.

handle_cast(_, State) -> {noreply, State}.

handle_info(_, State) -> {noreply, State}.

terminate(_Reason, _State) -> ok.

code_change(_, State, _) -> {ok, State}.

%% clock hasn't moved, increment sequence
get(Time, Time, ZoneId, WorkerId, Seq0, State) ->
    Sequence = Seq0 + 1,
    {{ok, flake_util:gen_id(Time, ZoneId, WorkerId, Sequence)}, State#state{sequence = Sequence}};
%% clock has progressed, reset sequence
get(CurrTime, MaxTime, ZoneId, WorkerId, _, State) when CurrTime > MaxTime ->
    {{ok, flake_util:gen_id(CurrTime, ZoneId, WorkerId, 0)}, State#state{max_time = CurrTime, sequence = 0}};
%% clock is running backwards
get(CurrTime, MaxTime, _ZoneId, _WorkerId, _Sequence, State) when MaxTime > CurrTime ->
    {{error, clock_running_backwards}, State}.
