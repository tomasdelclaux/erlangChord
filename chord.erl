-module(chord).
-compile(export_all).
-author("Tomas Delclaux Rodriguez-Rey and Ariel Weitzenfeld").

getHash(Data) ->  
    binary:decode_unsigned(crypto:hash(sha, Data)).

init()->
    {ok,[NumNodes, NumRequests]} = io:fread("", "~d~d"),
    start_actors(NumNodes,NumRequests),
    ok.

start_actors(0,_)->
    ok;

start_actors(NumNodes, NumRequests)->
    start(NumRequests),
    start_actors(NumNodes-1, NumRequests).

start(NumRequests)->
    ActorPid = spawn_link(?MODULE, loop, [NumRequests]),
    ok.
      
loop(NumRequests)->
    receive
        {lookup, Key} ->io:format("where is this key"),
        Num = NumRequests-1,
        case Num of
            0->%SEND MESSAGE TO TRACKER TO STOP;
                loop(NumRequests);
            _->loop(NumRequests-1)
        end;
        {terminate} -> io:format("received signal to stop")
    end,
    ok.

stop()->
    exit(self(),kill).



%% DO TRACKER SAME AS GOSSIP FOR PROGRAM TERMINATION