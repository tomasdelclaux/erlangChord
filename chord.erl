-module(chord).
-compile(export_all).
-author("Tomas Delclaux Rodriguez-Rey and Ariel Weitzenfeld").
-record(chord, {id, finger, successor, predecessor, key, numrequests}).
-record(tracker, {numNodes, finishedNodes}).

getHash(Data) ->  
    binary:decode_unsigned(crypto:hash(sha, Data)).

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
    ok.
      
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
track(Track)->
    receive
        {finish, Pid}->
            S2 = sets:add_element(Pid, Track#tracker.finishedNodes),
            NewTrack=Track#tracker{finishedNodes=S2},
            track(NewTrack)
    after 10 ->
        L = sets:to_list(Track#tracker.finishedNodes),
        NumActors=Track#tracker.numNodes,
        case length(L) of
            NumActors -> 
                io:format("All finished, need to avg calculation");
            _->track(Track)
        end
    end.

startTrack()->
    statistics(wall_clock),
    Set = sets:new(),
    Track= #tracker{numNodes=0, finishedNodes=Set},
    ActorPid = spawn_link(?MODULE, track, [Track]),
    register(list_to_atom("chord_tracker"), ActorPid).
