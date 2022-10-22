-module(chord).
-compile(export_all).
-author("Tomas Delclaux Rodriguez-Rey and Ariel Weitzenfeld").
-record(chord, {id, finger, successor, predecessor, key, numrequests}).
-record(tracker, {numNodes, finishedNodes}).
-define(M, 160).

getHash(Data) ->  
    binary:decode_unsigned(crypto:hash(sha, Data)).

% getNext(FingerTable)->
%     %%TODO

%% join()->
    %%TODO


%% need to implement stabilise too

init()->
    {ok,[NumNodes, NumRequests]} = 
        io:fread("Enter number of chord nodes and number of requests", "~d~d"),
    start_chord_nodes(NumNodes,NumRequests),
    ok.

start_chord_nodes(0,_)->
    ok;

start_chord_nodes(NumNodes, NumRequests)->
    start(NumRequests),
    start_chord_nodes(NumNodes-1, NumRequests).

start(NumRequests)->
    Chord=#chord{numrequests=NumRequests},
    ActorPid = spawn_link(?MODULE, loop, [Chord]),
    PidHash = getHash(pid_to_list(ActorPid)),
    register(PidHash, ActorPid),
    {ok, PidHash}.
      
chordNode(Chord)->
    receive
        {lookup, Key} ->io:format("where is this key"),
        Num=Chord#chord.numrequests,
        case Num of
            0->%SEND MESSAGE TO TRACKER TO STOP;
                chordNode(Chord);
            _->
                NewChord=Chord#chord{numrequests=Num-1},
                chordNode(NewChord)
        end;
        {terminate} -> io:format("received signal to stop")
    end,
    ok.

stop()->
    exit(self(),kill).


%%ACTOR TO KEEP TRACK OF FINISHING ACTORS
track(State)->
    receive
        {finish, Pid}->
            S2 = sets:add_element(Pid, State#tracker.finishedNodes),
            NewTrack=State#tracker{finishedNodes=S2},
            track(NewTrack)
    after 10 ->
        L = sets:to_list(State#tracker.finishedNodes),
        NumActors=State#tracker.numNodes,
        case length(L) of
            NumActors -> 
                io:format("All finished, need to avg calculation");
            _->track(State)
        end
    end.

startTrack()->
    statistics(wall_clock),
    Set = sets:new(),
    State= #tracker{numNodes=0, finishedNodes=Set},
    ActorPid = spawn_link(?MODULE, track, [State]),
    register(list_to_atom("chord_tracker"), ActorPid).
