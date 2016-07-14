%% vim:set softtabstop=4 shiftwidth=4 tabstop=4:
-module(emtenet_SUITE).
-compile(export_all).
-include_lib("common_test/include/ct.hrl").
-include_lib("../include/emysql.hrl").

-record(rank, {identity, sequence, title}).

-define(POOL, ?MODULE).

% List of test cases.
% Test cases have self explanatory names.
%%--------------------------------------------------------------------
all() -> 
    [ test_expect
	, test_outside
	, test_inside
	, test_duplicate
	, test_insert_many
	, test_by_title
	, test_move_up_first
	, test_move_up_second
	, test_move_up_third
	, test_move_down_tenth
	, test_move_down_ninth
	, test_move_down_eighth
	].


%%--------------------------------------------------------------------
init_per_suite(Config) ->

	% if this fails, focus on environment_SUITE to fix test setup.
	crypto:start(),
	application:start(emysql),
 
	emysql:add_pool(?POOL, 5,
		"hello_username", "hello_password", "localhost", 3306,
		"hello_database", utf8),

	Config.
    
end_per_suite(_) ->
	emysql:remove_pool(?POOL),
	ok.

%%--------------------------------------------------------------------
init_per_testcase(_Case, Config) ->
	emysql:execute(?POOL, <<"TRUNCATE TABLE rank">>),
	Config.

end_per_testcase(_Case, _Config) ->
	ok.

%%--------------------------------------------------------------------
%% Test that environment starts with empty table
test_expect(_) ->
	expect([]).

%%--------------------------------------------------------------------
%% Test that rank_insert() works outside of transactions
test_outside(_) ->
	Rank = #rank{title = <<"Title">>},
	rank_insert(?POOL, Rank),
	expect([<<"Title">>]).

%%--------------------------------------------------------------------
%% Test that rank_insert() works inside of transactions
test_inside(_) ->
	Transaction = fun (Pool) ->
		Rank = #rank{title = <<"Title">>},
		rank_insert(Pool, Rank)
	end,
	emysql:transaction(?POOL, Transaction),
	expect([<<"Title">>]).

%%--------------------------------------------------------------------
%% Test insert duplicate (abort)
test_duplicate(_) ->
	Rank = #rank{title = <<"Title">>},
	rank_insert(?POOL, Rank),
	Transaction = fun (Pool) ->
		rank_insert(Pool, Rank)
	end,
	Result = emysql:transaction(?POOL, Transaction),
	{aborted, {duplicate, _}} = Result.

%%--------------------------------------------------------------------
%% Test insert_many() utility
test_insert_many(_) ->
	insert_many(commit),
	expect(insert_many(titles)).

insert_many(titles) ->
	[ <<"First">>
	, <<"Second">>
	, <<"Third">>
	, <<"Fourth">>
	, <<"Fifth">>
	, <<"Sixth">>
	, <<"Seventh">>
	, <<"Eighth">>
	, <<"Ninth">>
	, <<"Tenth">>
	];
insert_many(commit) ->
	Transaction = fun (Pool) ->
		[ rank_insert(Pool, #rank{title = Title}) || Title <- insert_many(titles) ],
		ok
	end,
	{atomic, ok} = emysql:transaction(?POOL, Transaction),
	ok.

%%--------------------------------------------------------------------
%% Test rank_by_title
test_by_title(_) ->
	insert_many(commit),
	Rank = rank_by_title(?POOL, <<"First">>),
	#rank{sequence = 1, title = <<"First">>} = Rank.

%%--------------------------------------------------------------------
test_move_up_first(_) ->
	insert_many(commit),
	Transaction = fun (Pool) ->
		Rank = rank_by_title(Pool, <<"First">>),
		rank_move_up(Pool, Rank)
	end,
	{atomic, _} = emysql:transaction(?POOL, Transaction),
	expect(insert_many(titles)).

%%--------------------------------------------------------------------
test_move_up_second(_) ->
	insert_many(commit),
	Transaction = fun (Pool) ->
		Rank = rank_by_title(Pool, <<"Second">>),
		rank_move_up(Pool, Rank)
	end,
	{atomic, _} = emysql:transaction(?POOL, Transaction),
	[A, B | Titles] = insert_many(titles),
	expect([B, A | Titles]).

%%--------------------------------------------------------------------
test_move_up_third(_) ->
	insert_many(commit),
	Transaction = fun (Pool) ->
		Rank = rank_by_title(Pool, <<"Third">>),
		rank_move_up(Pool, Rank)
	end,
	{atomic, _} = emysql:transaction(?POOL, Transaction),
	[A, B, C | Titles] = insert_many(titles),
	expect([A, C, B | Titles]).

%%--------------------------------------------------------------------
test_move_down_tenth(_) ->
	insert_many(commit),
	Transaction = fun (Pool) ->
		Rank = rank_by_title(Pool, <<"Tenth">>),
		rank_move_down(Pool, Rank)
	end,
	{atomic, _} = emysql:transaction(?POOL, Transaction),
	expect(insert_many(titles)).

%%--------------------------------------------------------------------
test_move_down_ninth(_) ->
	insert_many(commit),
	Transaction = fun (Pool) ->
		Rank = rank_by_title(Pool, <<"Ninth">>),
		rank_move_down(Pool, Rank)
	end,
	{atomic, _} = emysql:transaction(?POOL, Transaction),
	[A, B | Titles] = lists:reverse(insert_many(titles)),
	expect(lists:reverse([B, A | Titles])).

%%--------------------------------------------------------------------
test_move_down_eighth(_) ->
	insert_many(commit),
	Transaction = fun (Pool) ->
		Rank = rank_by_title(Pool, <<"Eighth">>),
		rank_move_down(Pool, Rank)
	end,
	{atomic, _} = emysql:transaction(?POOL, Transaction),
	[A, B, C | Titles] = lists:reverse(insert_many(titles)),
	expect(lists:reverse([A, C, B | Titles])).

%%--------------------------------------------------------------------
rank_by_identity(Pool, Identity) ->
	Query = <<"select identity, sequence, title FROM rank WHERE identity = ?">>,
	Result = emysql:execute(Pool, Query, [Identity]),
	[Rank] = emysql_util:as_record(Result, rank, record_info(fields, rank)),
	Rank.

rank_by_sequence(Pool, Sequence) ->
	Query = <<"SELECT identity, sequence, title FROM rank WHERE sequence = ?">>,
	Result = emysql:execute(Pool, Query, [Sequence]),
	[Rank] = emysql_util:as_record(Result, rank, record_info(fields, rank)),
	Rank.

rank_by_title(Pool, Title) ->
	Query = <<"SELECT identity, sequence, title FROM rank WHERE title = ?">>,
	Result = emysql:execute(Pool, Query, [Title]),
	[Rank] = emysql_util:as_record(Result, rank, record_info(fields, rank)),
	Rank.

rank_insert(Pool, Rank = #rank{title = Title}) ->
	Insert = <<"INSERT INTO rank (sequence, title) VALUES (0, ?)">>,
	Identity = insert_id(emysql:execute(Pool, Insert, [Title])),
	Select = <<"SELECT MAX(sequence) FROM rank">>,
	Sequence = next_sequence(emysql:execute(Pool, Select)),
	Update = <<"UPDATE rank SET sequence = ? WHERE identity = ?">>,
	affected_row(emysql:execute(Pool, Update, [Sequence, Identity])),
	Rank#rank{identity = Identity, sequence = Sequence}.

rank_move_up(_Pool, Rank = #rank{sequence = 1}) ->
	Rank;
rank_move_up(Pool, Rank = #rank{identity = Identity, sequence = Sequence}) ->
	Aside = <<"UPDATE rank SET sequence = 0 WHERE identity = ?">>,
	affected_row(emysql:execute(Pool, Aside, [Identity])),
	Swap = <<"UPDATE rank SET sequence = ? WHERE sequence = ?">>,
	affected_row(emysql:execute(Pool, Swap, [Sequence, Sequence-1])),
	Restore = <<"UPDATE rank SET sequence = ? WHERE identity = ? AND sequence = 0">>,
	affected_row(emysql:execute(Pool, Restore, [Sequence-1, Identity])),
	Rank#rank{sequence = Sequence-1}.

rank_move_down(Pool, Rank = #rank{identity = Identity, sequence = Sequence}) ->
	Aside = <<"UPDATE rank SET sequence = 0 WHERE identity = ?">>,
	affected_row(emysql:execute(Pool, Aside, [Identity])),
	Swap = <<"UPDATE rank SET sequence = ? WHERE sequence = ?">>,
	case affected_rows(emysql:execute(Pool, Swap, [Sequence, Sequence+1])) of
		0 ->
			Restore = <<"UPDATE rank SET sequence = ? WHERE identity = ? AND sequence = 0">>,
			affected_row(emysql:execute(Pool, Restore, [Sequence, Identity])),
			Rank;
		1 ->
			Restore = <<"UPDATE rank SET sequence = ? WHERE identity = ? AND sequence = 0">>,
			affected_row(emysql:execute(Pool, Restore, [Sequence+1, Identity])),
			Rank#rank{sequence = Sequence+1}
	end.

insert_id(#error_packet{code = 1062, msg = Message}) ->
	emysql:abort({duplicate, Message});
insert_id(#ok_packet{insert_id = Identity}) ->
	Identity.

next_sequence(#result_packet{rows = [[Maximum]]}) ->
	Maximum + 1.

affected_row(#ok_packet{affected_rows = 1}) ->
	ok.

affected_rows(#ok_packet{affected_rows = Rows}) ->
	Rows.

%%--------------------------------------------------------------------
%% Check that database records are as expected
expect(Expect) ->
	% prepare the test
	Query = <<"SELECT title FROM rank ORDER BY sequence">>,
	#result_packet{rows = Rows} = emysql:execute(?POOL, Query),
	Actual = [ Title || [Title] <- Rows ],

	% the actual test
	case Actual of
		Expect ->
			invariant(Expect);
		_ ->
			error_logger:info_report(["expect failed", {expect, Expect}, {actual, Actual}]),
			throw(expect_failed)
	end.

%%--------------------------------------------------------------------
%% Check that database invariant holds
invariant([]) ->
	ok;
invariant(_) ->
	Query = <<"SELECT min(sequence), max(sequence), count(sequence) FROM rank">>,
	#result_packet{rows = [[Min, Max, Count]]} = emysql:execute(?POOL, Query),
	case {Min, Max, Count} of
		{1, Max, Max} ->
			ok;
		_ ->
			error_logger:info_report(["invariant failed", {min, Min}, {max, Max}, {count, Count}]),
			throw(invariant_failed)
	end.

