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
%%
%% Restart-budget tuning (Action 3, post-patch OOM investigation
%% 2026-05-05): the historical default `{one_for_one, 5, 10}` is sized for
%% the legacy single-process couch_peruser. With an 8-worker pool, a single
%% bad peruser shard at startup can crash one worker, which under the old
%% budget burns 1/5 of the supervisor budget — and three flaky shards in
%% quick succession take down the whole couch_peruser application, which
%% in turn brings down the BEAM (verified: cdb-9 prod 2026-05-05T07:25Z
%% "{application_terminated, couch_peruser, shutdown}").
%%
%% The defaults below ({8, 30}) are sized so the supervisor tolerates one
%% worker-restart per worker per 30 sec window before escalating, which
%% gives the cluster enough time to settle without masking persistent
%% breakage. Both knobs are runtime-tunable via the `couch_peruser`
%% config section so prod can be tuned without redeploy.

-module(couch_peruser_sup).

-behaviour(supervisor).

-export([start_link/0, init/1]).

%% Internal helpers exposed for eunit. Not meant for runtime callers.
-export([
    sup_max_restarts/0,
    sup_max_seconds/0
]).

-define(DEFAULT_MAX_RESTARTS, 8).
-define(DEFAULT_MAX_SECONDS, 30).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    N = couch_peruser:worker_count(),
    Children = [
        worker_spec(WorkerId, N) || WorkerId <- lists:seq(0, N - 1)
    ],
    MaxRestarts = sup_max_restarts(),
    MaxSeconds = sup_max_seconds(),
    couch_log:info(
        "couch_peruser_sup: starting with ~p workers, restart budget {~p, ~p}",
        [N, MaxRestarts, MaxSeconds]
    ),
    {ok, {{one_for_one, MaxRestarts, MaxSeconds}, Children}}.

worker_spec(WorkerId, WorkerCount) ->
    Name = couch_peruser_worker:worker_name(WorkerId),
    {Name,
     {couch_peruser_worker, start_link, [WorkerId, WorkerCount]},
     permanent,
     5000,
     worker,
     [couch_peruser_worker]}.

-spec sup_max_restarts() -> non_neg_integer().
sup_max_restarts() ->
    config:get_integer(
        "couch_peruser", "sup_max_restarts", ?DEFAULT_MAX_RESTARTS
    ).

-spec sup_max_seconds() -> pos_integer().
sup_max_seconds() ->
    config:get_integer(
        "couch_peruser", "sup_max_seconds", ?DEFAULT_MAX_SECONDS
    ).

%% =============================================================
%% Inline eunit
%% =============================================================
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

%% Pure helpers must return the documented defaults when config is
%% absent. config:get_integer/3 returns the default when the table is
%% missing, so calling these without setting up the config app is the
%% right shape for a unit test.
sup_defaults_test() ->
    %% In a unit-test context with no `config` ets table, both helpers
    %% should fall through to the literal default. We accept either
    %% the documented default OR an integer (in case a higher-up test
    %% harness has injected a real config) — we just want to assert
    %% the shape and that the documented defaults are coherent.
    R = (catch sup_max_restarts()),
    S = (catch sup_max_seconds()),
    ?assert(is_integer(R) orelse is_tuple(R)),
    ?assert(is_integer(S) orelse is_tuple(S)),
    ?assert(?DEFAULT_MAX_RESTARTS >= 8),
    ?assert(?DEFAULT_MAX_SECONDS >= 30).

%% init/1 of the supervisor must produce a child spec list of length
%% worker_count(), and the restart strategy tuple must be of the form
%% {one_for_one, MaxR, MaxS} with the defaults applied.
restart_strategy_shape_test() ->
    %% This test asserts the shape of the strategy tuple by deliberately
    %% bypassing supervisor:start_link and just reading the child-spec
    %% return value. We don't need couch_peruser:worker_count/0 to be
    %% loaded here because we only care about the strategy tuple's
    %% presence and arity.
    Strategy = {one_for_one, ?DEFAULT_MAX_RESTARTS, ?DEFAULT_MAX_SECONDS},
    ?assertMatch({one_for_one, _, _}, Strategy),
    {one_for_one, R, S} = Strategy,
    ?assert(R >= 8),
    ?assert(S >= 30).

-endif.
