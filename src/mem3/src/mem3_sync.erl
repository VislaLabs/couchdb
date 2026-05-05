% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(mem3_sync).
-behaviour(gen_server).
-vsn(1).
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-export([
    start_link/0,
    get_active/0,
    get_queue/0,
    push/1, push/2,
    remove_node/1,
    remove_shard/1,
    initial_sync/1,
    get_backlog/0,
    nodes_db/0,
    shards_db/0,
    users_db/0,
    find_next_node/0
]).
-export([
    local_dbs/0
]).

-import(queue, [in/2, out/1, to_list/1, join/2, from_list/1, is_empty/1]).

-include_lib("mem3/include/mem3.hrl").
-include_lib("couch/include/couch_db.hrl").
-include_lib("kernel/include/file.hrl").

-record(state, {
    active = [],
    count = 0,
    limit,
    dict = dict:new(),
    waiting = queue:new()
}).

-record(job, {name, node, count = nil, pid = nil}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

get_active() ->
    gen_server:call(?MODULE, get_active).

get_queue() ->
    gen_server:call(?MODULE, get_queue).

get_backlog() ->
    gen_server:call(?MODULE, get_backlog).

push(#shard{name = Name}, Target) ->
    push(Name, Target);
push(Name, #shard{node = Node}) ->
    push(Name, Node);
push(Name, Node) ->
    push(#job{name = Name, node = Node}).

push(#job{node = Node} = Job) when Node =/= node() ->
    gen_server:cast(?MODULE, {push, Job});
push(_) ->
    ok.

remove_node(Node) ->
    gen_server:cast(?MODULE, {remove_node, Node}).

remove_shard(Shard) ->
    gen_server:cast(?MODULE, {remove_shard, Shard}).

init([]) ->
    process_flag(trap_exit, true),
    Concurrency = config:get("mem3", "sync_concurrency", "10"),
    gen_event:add_handler(mem3_events, mem3_sync_event, []),
    initial_sync(),
    {ok, #state{limit = list_to_integer(Concurrency)}}.

handle_call({push, Job}, From, State) ->
    handle_cast({push, Job#job{pid = From}}, State);
handle_call(get_active, _From, State) ->
    {reply, State#state.active, State};
handle_call(get_queue, _From, State) ->
    {reply, to_list(State#state.waiting), State};
handle_call(get_backlog, _From, #state{active = A, waiting = WQ} = State) ->
    CA = lists:sum([C || #job{count = C} <- A, is_integer(C)]),
    CW = lists:sum([C || #job{count = C} <- to_list(WQ), is_integer(C)]),
    {reply, CA + CW, State}.

handle_cast({push, DbName, Node}, State) ->
    handle_cast({push, #job{name = DbName, node = Node}}, State);
handle_cast({push, #job{name = DbName} = Job}, State) ->
    case is_dormant_peruser_shard(DbName) of
        true ->
            % Skip dormant peruser shards: their .couch file has not been
            % touched on disk for [mem3] active_peruser_age_days days. This
            % bounds mem3_sync work by the active working set and prevents
            % binary_alloc carrier fragmentation OOM on cold start when there
            % are O(100k) per-user databases.
            couch_log:debug(
                "mem3_sync: skipping dormant peruser shard ~s",
                [DbName]
            ),
            {noreply, State};
        false ->
            handle_push(Job, State)
    end;
handle_cast({remove_node, Node}, #state{waiting = W0} = State) ->
    {Alive, Dead} = lists:partition(fun(#job{node = N}) -> N =/= Node end, to_list(W0)),
    Dict = remove_entries(State#state.dict, Dead),
    [
        exit(Pid, die_now)
     || #job{node = N, pid = Pid} <- State#state.active,
        N =:= Node
    ],
    {noreply, State#state{dict = Dict, waiting = from_list(Alive)}};
handle_cast({remove_shard, Shard}, #state{waiting = W0} = State) ->
    {Alive, Dead} = lists:partition(
        fun(#job{name = S}) ->
            S =/= Shard
        end,
        to_list(W0)
    ),
    Dict = remove_entries(State#state.dict, Dead),
    [
        exit(Pid, die_now)
     || #job{name = S, pid = Pid} <- State#state.active,
        S =:= Shard
    ],
    {noreply, State#state{dict = Dict, waiting = from_list(Alive)}}.

handle_push(Job, #state{count = Count, limit = Limit} = State) when Count >= Limit ->
    {noreply, add_to_queue(State, Job)};
handle_push(Job, State) ->
    #state{active = L, count = C} = State,
    #job{name = DbName, node = Node} = Job,
    case is_running(DbName, Node, L) of
        true ->
            {noreply, add_to_queue(State, Job)};
        false ->
            Pid = start_push_replication(Job),
            {noreply, State#state{active = [Job#job{pid = Pid} | L], count = C + 1}}
    end.

handle_info({'EXIT', Active, normal}, State) ->
    handle_replication_exit(State, Active);
handle_info({'EXIT', Active, die_now}, State) ->
    % we forced this one ourselves, do not retry
    handle_replication_exit(State, Active);
handle_info({'EXIT', Active, {{not_found, no_db_file}, _Stack}}, State) ->
    % target doesn't exist, do not retry
    handle_replication_exit(State, Active);
handle_info({'EXIT', Active, Reason}, State) ->
    NewState =
        case lists:keyfind(Active, #job.pid, State#state.active) of
            #job{name = OldDbName, node = OldNode} = Job ->
                couch_log:warning("~s ~s ~s ~w", [?MODULE, OldDbName, OldNode, Reason]),
                case Reason of
                    {pending_changes, Count} ->
                        maybe_resubmit(State, Job#job{pid = nil, count = Count});
                    _ ->
                        case mem3:db_is_current(Job#job.name) of
                            true ->
                                timer:apply_after(5000, ?MODULE, push, [Job#job{pid = nil}]);
                            false ->
                                % no need to retry (db deleted or recreated)
                                ok
                        end,
                        State
                end;
            false ->
                State
        end,
    handle_replication_exit(NewState, Active);
handle_info(Msg, State) ->
    couch_log:notice("unexpected msg at replication manager ~p", [Msg]),
    {noreply, State}.

terminate(_Reason, State) ->
    [exit(Pid, shutdown) || #job{pid = Pid} <- State#state.active],
    ok.

code_change(_, #state{waiting = WaitingList} = State, _) when is_list(WaitingList) ->
    {ok, State#state{waiting = from_list(WaitingList)}};
code_change(_, State, _) ->
    {ok, State}.

maybe_resubmit(State, #job{name = DbName, node = Node} = Job) ->
    case lists:member(DbName, local_dbs()) of
        true ->
            case find_next_node() of
                Node ->
                    add_to_queue(State, Job);
                _ ->
                    % don't resubmit b/c we have a new replication target
                    State
            end;
        false ->
            add_to_queue(State, Job)
    end.

handle_replication_exit(State, Pid) ->
    #state{active = Active, limit = Limit, dict = D, waiting = Waiting} = State,
    Active1 = lists:keydelete(Pid, #job.pid, Active),
    case is_empty(Waiting) of
        true ->
            {noreply, State#state{active = Active1, count = length(Active1)}};
        _ ->
            Count = length(Active1),
            NewState =
                if
                    Count < Limit ->
                        case next_replication(Active1, Waiting, queue:new()) of
                            % all waiting replications are also active
                            nil ->
                                State#state{active = Active1, count = Count};
                            {#job{name = DbName, node = Node} = Job, StillWaiting} ->
                                NewPid = start_push_replication(Job),
                                State#state{
                                    active = [Job#job{pid = NewPid} | Active1],
                                    count = Count + 1,
                                    dict = dict:erase({DbName, Node}, D),
                                    waiting = StillWaiting
                                }
                        end;
                    true ->
                        State#state{active = Active1, count = Count}
                end,
            {noreply, NewState}
    end.

start_push_replication(#job{name = Name, node = Node, pid = From}) ->
    if
        From =/= nil -> gen_server:reply(From, ok);
        true -> ok
    end,
    spawn_link(fun() ->
        case mem3_rep:go(Name, maybe_redirect(Node)) of
            {ok, Pending} when Pending > 0 ->
                exit({pending_changes, Pending});
            _ ->
                ok
        end
    end).

add_to_queue(State, #job{name = DbName, node = Node, pid = From} = Job) ->
    #state{dict = D, waiting = WQ} = State,
    case dict:is_key({DbName, Node}, D) of
        true ->
            if
                From =/= nil -> gen_server:reply(From, ok);
                true -> ok
            end,
            State;
        false ->
            couch_log:debug("adding ~s -> ~p to mem3_sync queue", [DbName, Node]),
            State#state{
                dict = dict:store({DbName, Node}, ok, D),
                waiting = in(Job, WQ)
            }
    end.

sync_nodes_and_dbs() ->
    Node = find_next_node(),
    [push(Db, Node) || Db <- local_dbs()].

initial_sync() ->
    mem3_sync_nodes:add(nodes()).

initial_sync(Live) ->
    sync_nodes_and_dbs(),
    Acc = {node(), Live, []},
    {_, _, Shards} = mem3_shards:fold(fun initial_sync_fold/2, Acc),
    submit_replication_tasks(node(), Live, Shards).

initial_sync_fold(#shard{dbname = Db} = Shard, {LocalNode, Live, AccShards}) ->
    case AccShards of
        [#shard{dbname = AccDb} | _] when Db =/= AccDb ->
            submit_replication_tasks(LocalNode, Live, AccShards),
            {LocalNode, Live, [Shard]};
        _ ->
            {LocalNode, Live, [Shard | AccShards]}
    end.

submit_replication_tasks(LocalNode, Live, Shards) ->
    SplitFun = fun(#shard{node = Node}) -> Node =:= LocalNode end,
    {Local0, Remote} = lists:partition(SplitFun, Shards),
    %% Drop dormant peruser shards (braid-<id> whose .couch file has not been
    %% touched on disk for [mem3] active_peruser_age_days days, default 7).
    %% This bounds initial_sync work by the active working set rather than the
    %% total per-user shard count, eliminating binary_alloc carrier
    %% fragmentation that OOMs the BEAM at startup on hubs with O(100k)
    %% per-user databases.
    Local = filter_dormant_peruser_shards(Local0),
    case length(Local0) - length(Local) of
        0 ->
            ok;
        Skipped ->
            couch_log:notice(
                "mem3_sync: skipped ~b dormant peruser shards in initial_sync",
                [Skipped]
            )
    end,
    lists:foreach(
        fun(#shard{name = ShardName}) ->
            [
                sync_push(ShardName, N)
             || #shard{node = N, name = Name} <- Remote,
                Name =:= ShardName,
                lists:member(N, Live)
            ]
        end,
        Local
    ).

sync_push(ShardName, N) ->
    gen_server:call(mem3_sync, {push, #job{name = ShardName, node = N}}, infinity).

find_next_node() ->
    LiveNodes = [node() | nodes()],
    AllNodes0 = lists:sort(mem3:nodes()),
    AllNodes1 = [X || X <- AllNodes0, lists:member(X, LiveNodes)],
    AllNodes = AllNodes1 ++ [hd(AllNodes1)],
    [_Self, Next | _] = lists:dropwhile(fun(N) -> N =/= node() end, AllNodes),
    Next.

%% @doc Finds the next {DbName,Node} pair in the list of waiting replications
%% which does not correspond to an already running replication
-spec next_replication([#job{}], queue:queue(_), queue:queue(_)) ->
    {#job{}, queue:queue(_)} | nil.
next_replication(Active, Waiting, WaitingAndRunning) ->
    case is_empty(Waiting) of
        true ->
            nil;
        false ->
            {{value, #job{name = S, node = N} = Job}, RemQ} = out(Waiting),
            case is_running(S, N, Active) of
                true ->
                    next_replication(Active, RemQ, in(Job, WaitingAndRunning));
                false ->
                    {Job, join(RemQ, WaitingAndRunning)}
            end
    end.

is_running(DbName, Node, ActiveList) ->
    [] =/= [true || #job{name = S, node = N} <- ActiveList, S =:= DbName, N =:= Node].

remove_entries(Dict, Entries) ->
    lists:foldl(
        fun(#job{name = S, node = N}, D) ->
            dict:erase({S, N}, D)
        end,
        Dict,
        Entries
    ).

local_dbs() ->
    UsersDb = users_db(),
    % users db might not have been created so don't include it unless it exists
    case couch_server:exists(UsersDb) of
        true -> [nodes_db(), shards_db(), UsersDb];
        false -> [nodes_db(), shards_db()]
    end.

nodes_db() ->
    ?l2b(config:get("mem3", "nodes_db", "_nodes")).

shards_db() ->
    ?l2b(config:get("mem3", "shards_db", "_dbs")).

users_db() ->
    ?l2b(config:get("couch_httpd_auth", "authentication_db", "_users")).

maybe_redirect(Node) ->
    case config:get("mem3.redirects", atom_to_list(Node)) of
        undefined ->
            Node;
        Redirect ->
            couch_log:debug("Redirecting push from ~p to ~p", [Node, Redirect]),
            list_to_existing_atom(Redirect)
    end.

%% =============================================================================
%% Dormant peruser shard filtering
%% =============================================================================
%%
%% Per-user databases (couch_peruser, name pattern <<"braid-<id>">>) accumulate
%% to O(100k+) on long-lived hubs. On every cdb cold start, mem3_sync walks
%% q*n*PeruserCount shard pairs through the replication queue. Even when the
%% queue is bounded by sync_concurrency, each candidate triggers a refc-binary
%% allocation for the shard name and metadata. Those binaries fragment
%% binary_alloc carriers that BEAM never returns to the OS, which OOMs the
%% process at ~80GB while erlang:memory/0 reports a few hundred MB.
%%
%% The fix here bounds mem3_sync work to the *active* peruser working set:
%% any peruser shard whose .couch file has not been touched on disk for
%% [mem3] active_peruser_age_days days is skipped at enqueue time. Non-peruser
%% shards (_users, _dbs, _nodes, regular sharded DBs) are NEVER skipped.
%% On stat error, the shard is KEPT (conservative — never silently lose work).

-define(PERUSER_PREFIX, <<"braid-">>).
-define(DEFAULT_ACTIVE_AGE_DAYS, 7).

%% @doc Filter a list of #shard{} records, dropping peruser shards whose
%% on-disk .couch file mtime is older than the configured active age.
-spec filter_dormant_peruser_shards([#shard{}]) -> [#shard{}].
filter_dormant_peruser_shards(Shards) ->
    AgeDays = active_peruser_age_days(),
    case AgeDays of
        0 ->
            %% 0 disables the filter entirely. Useful for emergency rollback
            %% via runtime config without redeploying.
            Shards;
        _ ->
            CutoffSecs = erlang:system_time(second) - (AgeDays * 86400),
            [S || S <- Shards, not is_dormant_peruser_shard_record(S, CutoffSecs)]
    end.

%% @doc True if the shard name belongs to a peruser db AND its .couch file
%% mtime is older than the active cutoff. This is the variant used on the
%% push/cast path where we have a shard-name binary, not the #shard{} record.
-spec is_dormant_peruser_shard(binary() | string() | atom()) -> boolean().
is_dormant_peruser_shard(ShardName) when is_binary(ShardName) ->
    case is_peruser_shard_name(ShardName) of
        false ->
            false;
        true ->
            AgeDays = active_peruser_age_days(),
            case AgeDays of
                0 ->
                    false;
                _ ->
                    CutoffSecs = erlang:system_time(second) - (AgeDays * 86400),
                    is_dormant_by_mtime(ShardName, CutoffSecs)
            end
    end;
is_dormant_peruser_shard(ShardName) when is_list(ShardName) ->
    is_dormant_peruser_shard(list_to_binary(ShardName));
is_dormant_peruser_shard(_) ->
    false.

is_dormant_peruser_shard_record(#shard{name = Name, dbname = DbName}, CutoffSecs) ->
    case is_peruser_dbname(DbName) of
        false ->
            false;
        true ->
            is_dormant_by_mtime(Name, CutoffSecs)
    end.

is_peruser_shard_name(<<"shards/", _:8/binary, "-", _:8/binary, "/", Rest/binary>>) ->
    is_peruser_dbname_with_suffix(Rest);
is_peruser_shard_name(_) ->
    false.

is_peruser_dbname_with_suffix(<<"braid-", _/binary>>) ->
    true;
is_peruser_dbname_with_suffix(_) ->
    false.

is_peruser_dbname(DbName) when is_binary(DbName) ->
    case DbName of
        <<"braid-", _/binary>> -> true;
        _ -> false
    end;
is_peruser_dbname(_) ->
    false.

%% @doc Returns true if the shard's .couch file exists and mtime < CutoffSecs.
%% Returns false (KEEP shard) on any stat error or if the file mtime is
%% recent enough.
-spec is_dormant_by_mtime(binary(), integer()) -> boolean().
is_dormant_by_mtime(ShardName, CutoffSecs) ->
    Path = shard_file_path(ShardName),
    case file:read_file_info(Path, [{time, posix}]) of
        {ok, #file_info{mtime = MTime}} when is_integer(MTime) ->
            MTime < CutoffSecs;
        _ ->
            %% File missing, permission denied, or any other error: be
            %% conservative and KEEP the shard (do NOT silently skip).
            false
    end.

shard_file_path(ShardName) when is_binary(ShardName) ->
    shard_file_path(binary_to_list(ShardName));
shard_file_path(ShardName) when is_list(ShardName) ->
    RootDir = config:get("couchdb", "database_dir", "."),
    filename:join([RootDir, "./" ++ ShardName ++ ".couch"]).

active_peruser_age_days() ->
    config:get_integer("mem3", "active_peruser_age_days", ?DEFAULT_ACTIVE_AGE_DAYS).

%% =============================================================================
%% Tests
%% =============================================================================

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

is_peruser_shard_name_test_() ->
    [
        ?_assert(is_peruser_shard_name(
            <<"shards/00000000-7fffffff/braid-abc123.1234567890">>
        )),
        ?_assert(is_peruser_shard_name(
            <<"shards/80000000-ffffffff/braid-x.0">>
        )),
        ?_assertNot(is_peruser_shard_name(
            <<"shards/00000000-7fffffff/_users.1234567890">>
        )),
        ?_assertNot(is_peruser_shard_name(
            <<"shards/00000000-7fffffff/_dbs.1234567890">>
        )),
        ?_assertNot(is_peruser_shard_name(
            <<"shards/00000000-7fffffff/some_org_db.1234567890">>
        )),
        ?_assertNot(is_peruser_shard_name(<<"_users">>)),
        ?_assertNot(is_peruser_shard_name(<<"braid-not-a-shard">>)),
        ?_assertNot(is_peruser_shard_name(<<"">>))
    ].

is_peruser_dbname_test_() ->
    [
        ?_assert(is_peruser_dbname(<<"braid-abc">>)),
        ?_assert(is_peruser_dbname(<<"braid-">>)),
        ?_assertNot(is_peruser_dbname(<<"_users">>)),
        ?_assertNot(is_peruser_dbname(<<"_dbs">>)),
        ?_assertNot(is_peruser_dbname(<<"braid">>)),
        ?_assertNot(is_peruser_dbname(<<"foo">>)),
        ?_assertNot(is_peruser_dbname(undefined))
    ].

is_dormant_by_mtime_test_() ->
    {
        setup,
        fun() ->
            Dir = filename:join(["/tmp", "mem3_sync_filter_test"]),
            os:cmd("rm -rf " ++ Dir),
            ok = filelib:ensure_dir(filename:join([Dir, "shards/00000000-7fffffff/x"])),
            meck:new(config, [passthrough]),
            meck:expect(config, get, fun
                ("couchdb", "database_dir", _) -> Dir;
                (S, K, D) -> meck:passthrough([S, K, D])
            end),
            Dir
        end,
        fun(Dir) ->
            meck:unload(config),
            os:cmd("rm -rf " ++ Dir)
        end,
        fun(Dir) ->
            ShardName = <<"shards/00000000-7fffffff/braid-active.1234">>,
            File = filename:join([Dir, "./shards/00000000-7fffffff/braid-active.1234.couch"]),
            ok = filelib:ensure_dir(File),
            ok = file:write_file(File, <<>>),
            Now = erlang:system_time(second),
            MissingShard = <<"shards/00000000-7fffffff/braid-missing.1234">>,
            [
                %% Recent (just-written) file vs cutoff in the past: KEEP.
                ?_assertNot(is_dormant_by_mtime(ShardName, Now - 86400)),
                %% Recent file vs cutoff far in the future: DORMANT.
                ?_assert(is_dormant_by_mtime(ShardName, Now + 86400)),
                %% Missing file: KEEP (conservative — never silently skip).
                ?_assertNot(is_dormant_by_mtime(MissingShard, Now + 86400))
            ]
        end
    }.

filter_dormant_peruser_shards_test_() ->
    {
        setup,
        fun() ->
            meck:new(config, [passthrough]),
            meck:expect(config, get_integer, fun
                ("mem3", "active_peruser_age_days", _) -> 0;
                (S, K, D) -> meck:passthrough([S, K, D])
            end)
        end,
        fun(_) -> meck:unload(config) end,
        fun(_) ->
            Shards = [
                #shard{name = <<"shards/0/braid-x.1">>, dbname = <<"braid-x">>},
                #shard{name = <<"shards/0/_users.1">>, dbname = <<"_users">>}
            ],
            [
                %% age=0 disables the filter: all shards pass through.
                ?_assertEqual(Shards, filter_dormant_peruser_shards(Shards))
            ]
        end
    }.

-endif.
