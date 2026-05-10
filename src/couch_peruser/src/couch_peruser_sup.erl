% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
% http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

%% couch_peruser supervisor.
%%
%% Starts N independent couch_peruser_worker children. Each worker owns a
%% disjoint slice of local _users shards. Replaces the previous singleton
%% gen_server (single-mailbox bottleneck) with a fixed pool whose aggregate
%% throughput scales with N.

-module(couch_peruser_sup).

-behaviour(supervisor).

-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    N = couch_peruser:worker_count(),
    Children = [
        worker_spec(WorkerId, N) || WorkerId <- lists:seq(0, N - 1)
    ],
    {ok, {{one_for_one, 5, 10}, Children}}.

worker_spec(WorkerId, WorkerCount) ->
    Name = couch_peruser_worker:worker_name(WorkerId),
    {Name,
     {couch_peruser_worker, start_link, [WorkerId, WorkerCount]},
     permanent,
     5000,
     worker,
     [couch_peruser_worker]}.
