%% vim:set softtabstop=4 shiftwidth=4 tabstop=4:
-module(emtenet_SUITE).
-compile(export_all).
-include_lib("common_test/include/ct.hrl").
-include_lib("../include/emysql.hrl").

-record(rank, {identity, sequence, title}).

% List of test cases.
% Test cases have self explanatory names.
%%--------------------------------------------------------------------
all() -> 
    [ test ].


%%--------------------------------------------------------------------
init_per_suite(Config) ->

	% if this fails, focus on environment_SUITE to fix test setup.
	crypto:start(),
	application:start(emysql),
 
	emysql:add_pool(pool, 5,
		"hello_username", "hello_password", "localhost", 3306,
		"hello_database", utf8),

	emysql:execute(pool, <<"TRUNCATE TABLE rank">>),

	Config.
    
end_per_suite(_) ->
	ok.

%%--------------------------------------------------------------------
init_per_testcase(_Case, Config) ->
	Config.

end_per_testcase(_Case, _Config) ->
	ok.

%%--------------------------------------------------------------------
test(_) ->
	ok. %emysql:transaction(pool,

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

rank_insert(Pool, Title) ->
	Query1 = <<"INSERT INTO rank (sequence, title) VALUES (0, ?)">>,
	Result1 = emysql:execute(Pool, Query1, [Title]),
	Identity = Result1#ok_packet.insert_id,
	Query2 = <<"SELECT MAX(sequence) FROM rank">>,
	Result2 = emysql:execute(Pool, Query2),
	Sequence = 1 + hd(hd(Result2#result_packet.rows)),
	Query3 = <<"UPDATE rank SET sequence = ? WHERE identity = ?">>,
	Result3 = emysql:execute(Pool, Query3, [Sequence, Identity]),
	#ok_packet{affected_rows=1} = Result3,
	#rank{identity=Identity, sequence=Sequence, title=Title}.

