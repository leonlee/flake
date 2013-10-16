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

-module(flake_util).
-author('Dietrich Featherston <d@boundary.com>').

-export([
    get_if_hw_int/1,
    hw_addr_to_int/1,
    curr_time_millis/0,
    gen_id/4
]).

-include_lib("eunit/include/eunit.hrl").

%% tweak epoch to 2013-07-04
-define(TW_EPOCH, 1372867200000).

%% get the mac/hardware address of the given interface as a 48-bit integer
get_if_hw_int(undefined) ->
    {error, if_not_found};
get_if_hw_int(IfName) ->
    {ok, IfAddrs} = inet:getifaddrs(),
    IfProps = proplists:get_value(IfName, IfAddrs),
    case IfProps of
        undefined ->
            {error, if_not_found};
        _ ->
            HwAddr = proplists:get_value(hwaddr, IfProps),
            {ok, hw_addr_to_int(HwAddr)}
    end.

%% convert an array of 6 bytes into a 48-bit integer
hw_addr_to_int(HwAddr) ->
    <<WorkerId:48/integer>> = erlang:list_to_binary(HwAddr),
    WorkerId.

curr_time_millis() ->
    {MegaSec, Sec, MicroSec} = os:timestamp(),
    (1000000000 * MegaSec + Sec * 1000 + MicroSec div 1000) - ?TW_EPOCH.

gen_id(Time, ZoneId, WorkerId, Sequence) ->
    <<Time:48/integer, ZoneId:16/integer, WorkerId:48/integer, Sequence:16/integer>>.


%% ----------------------------------------------------------
%% tests
%% ----------------------------------------------------------

flake_test() ->
    TS = flake_util:curr_time_millis(),
    Worker = flake_util:hw_addr_to_int(lists:seq(1, 6)),
    ZoneId = 1,
    Flake = flake_util:gen_id(TS, ZoneId, Worker, 0),
    <<Time:48/integer, ZoneId:16/integer, WorkerId:48/integer, Sequence:16/integer>> = Flake,
    ?assert(?debugVal(Time) =:= TS),
    ?assert(?debugVal(ZoneId) =:= ZoneId),
    ?assert(?debugVal(Worker) =:= WorkerId),
    ?assert(?debugVal(Sequence) =:= 0),
    <<FlakeInt:128/integer>> = Flake,
    ?debugVal(xor_util:as_list(FlakeInt, 62)),
    ok.
