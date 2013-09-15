%% vim:set softtabstop=4 shiftwidth=4 tabstop=4:
-module(transaction_SUITE).
-compile(export_all).
-include_lib("common_test/include/ct.hrl").
-include_lib("../include/emysql.hrl").

-define(POOL, ?MODULE).

% List of test cases.
% Test cases have self explanatory names.
%%--------------------------------------------------------------------
all() -> 
    [ emysql_transaction
	, emysql_execute
	, environment_expect
	, successful_commit
	, failure_by_exit
	, failure_by_abort
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
	emysql:execute(?POOL, <<"TRUNCATE TABLE `uniq`">>),
	emysql:execute(?POOL, <<"INSERT INTO `uniq` VALUES (1,1), (2,2)">>),
	Config.

end_per_testcase(_Case, _Config) ->
	ok.

%%--------------------------------------------------------------------
%% check existance of emysql:transaction()
%% simplest cast
emysql_transaction(_) ->
	Fun = fun (_Pool) ->
		ok
	end,
	{atomic, ok} = emysql:transaction(?POOL, Fun).

%%--------------------------------------------------------------------
%% check emysql:execute() can take pool as an atom or a connection record
%% SQL not of interest
emysql_execute(_) ->
	Fun = fun(Pool) ->
		emysql:execute(Pool, <<"SELECT 1">>),
		ok
	end,
	{atomic, ok} = emysql:transaction(?POOL, Fun).

%%--------------------------------------------------------------------
%% test the expect() utility function in the testcase environment
environment_expect(_) ->
	expect([[1,1],[2,2]]).

%%--------------------------------------------------------------------
%% check that statements are committed
successful_commit(_) ->
	Fun = fun(Pool) ->
		emysql:execute(Pool, <<"INSERT INTO `uniq` VALUES (3,3)">>)
	end,
	{atomic, #ok_packet{}} = emysql:transaction(?POOL, Fun),
	expect([[1,1],[2,2],[3,3]]).

%%--------------------------------------------------------------------
%% check that statements are NOT committed when an exit() occurs
failure_by_exit(_) ->
	Fun = fun(Pool) ->
		emysql:execute(Pool, <<"INSERT INTO `uniq` VALUES (3,3)">>),
		exit(reason)
	end,
	try
		emysql:transaction(?POOL, Fun)
	catch
		exit:{reason, {}} ->
			%% reason is wrapped by {Reason, {}}
			%% in emysql:monitor_work (line 617)
			ok
	end,
	expect([[1,1],[2,2]]).

%%--------------------------------------------------------------------
%% check that statements are NOT committed when an emysql:abort() occurs
failure_by_abort(_) ->
	Fun = fun(Pool) ->
		emysql:execute(Pool, <<"INSERT INTO `uniq` VALUES (3,3)">>),
		emysql:abort(reason)
	end,
	{aborted, reason} = emysql:transaction(?POOL, Fun),
	expect([[1,1],[2,2]]).

%%--------------------------------------------------------------------
%% Check that the database records are as expected
expect(Expect) ->
	% prepare for the test
	Query = <<"SELECT * FROM `uniq` ORDER BY `key`">>,
	#result_packet{rows=Select} = emysql:execute(?POOL, Query),
	
	% the actual test
	Expect = Select,

	ok.
