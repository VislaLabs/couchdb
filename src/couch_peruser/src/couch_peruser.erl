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

%% couch_peruser - public API + helpers.
%%
%% Historically this module was a singleton gen_server registered as
%% `couch_peruser`. Under sustained _users churn on large clusters its
%% mailbox grew unbounded (verified > 130M messages in prod), pinning
%% refc-binaries and triggering OOM kills.
%%
%% The actual gen_server logic now lives in couch_peruser_worker. The
%% supervisor (couch_peruser_sup) starts N independent workers, each owning
%% 1/N of the local _users shards via phash2(ShardName, N) =:= WorkerId.
%% This module is a thin facade exposing helpers that fan out to / aggregate
%% across the workers.

-module(couch_peruser).

-define(DEFAULT_WORKER_COUNT, 8).
-define(MIN_WORKER_COUNT, 1).
-define(MAX_WORKER_COUNT, 64).

-export([
    worker_count/0,
    workers/0,
    is_stable/0,
    mailbox_lengths/0
]).

%% Number of peruser workers configured for this node.
%%
%% Read from `[couch_peruser] worker_count`, defaulting to 8. Clamped to
%% [1, 64] to keep supervision predictable.
-spec worker_count() -> pos_integer().
worker_count() ->
    Configured = config:get_integer(
        "couch_peruser", "worker_count", ?DEFAULT_WORKER_COUNT
    ),
    Clamped = max(?MIN_WORKER_COUNT, min(?MAX_WORKER_COUNT, Configured)),
    Clamped.

%% List of registered worker names.
-spec workers() -> [atom()].
workers() ->
    N = worker_count(),
    [couch_peruser_worker:worker_name(I) || I <- lists:seq(0, N - 1)].

%% True iff every running worker reports cluster_stable.
%%
%% A worker that is not yet registered (e.g. mid-boot) is treated as
%% unstable, matching the historical singleton behaviour where calls to a
%% not-yet-started gen_server returned via timeout.
-spec is_stable() -> boolean().
is_stable() ->
    lists:all(
        fun(Name) ->
            case whereis(Name) of
                undefined ->
                    false;
                Pid when is_pid(Pid) ->
                    try
                        couch_peruser_worker:is_stable(Pid)
                    catch
                        exit:{noproc, _} -> false;
                        exit:{timeout, _} -> false
                    end
            end
        end,
        workers()
    ).

%% Diagnostic helper: returns [{WorkerName, MailboxLen}] for each worker.
%% Used by tests + on-call to verify the dispatcher is sharding correctly.
-spec mailbox_lengths() -> [{atom(), non_neg_integer() | undefined}].
mailbox_lengths() ->
    [
        {Name,
         case whereis(Name) of
             undefined -> undefined;
             Pid ->
                 case process_info(Pid, message_queue_len) of
                     {message_queue_len, L} -> L;
                     undefined -> undefined
                 end
         end}
        || Name <- workers()
    ].
