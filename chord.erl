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

create_finger_table(_,0)->
    [];

create_finger_table(ID,M)->
    [ID|create_finger_table(ID,M-1)].

finger_print([])-> 
    [];

finger_print([H|T]) ->
    io:format("    ~p~n", [H]),
    [H|finger_print(T)].


init()->
    {ok,[NumNodes, NumRequests]} = 
        io:fread("Enter number of chord nodes and number of requests", "~d~d"),
    ok.

create(NumRequests)->
    Chord=#chord{},
    NodePid = spawn_link(?MODULE, chordNode, [Chord]),
    ID=getHash(pid_to_list(NodePid)),
    register(list_to_atom(integer_to_list(ID)), NodePid),
    Finger=create_finger_table(ID,?M),
    Key=getHash("1"),
    String="I am node 1",
    Map=#{Key=> String},
    UpdateChord = #chord{id=ID, finger=Finger, successor=ID, key=Map, numrequests=NumRequests},
    NodePid ! {update, UpdateChord},
    ok.

% join(Node, )

% start(NumRequests)->
%     Chord=#chord{numrequests=NumRequests},
%     ActorPid = spawn_link(?MODULE, loop, [Chord]),
%     PidHash = getHash(pid_to_list(ActorPid)),
%     register(PidHash, ActorPid),
%     ok.
printNode(Chord)->
    io:format("OUTPUTTING STATE OF NODE:\n"),
    io:format("ID: ~w~n", [Chord#chord.id]),
    io:format("Finger~n"),
    finger_print(Chord#chord.finger),
    io:format("Predecessor: ~w~n", [Chord#chord.predecessor]),
    io:format("Successor: ~w~n", [Chord#chord.successor]),
    io:format(lists:flatten(io_lib:format("~p", [maps:to_list(Chord#chord.key)]))),
    io:format("~nnumRequests: ~w~n", [Chord#chord.numrequests]).

chordNode(Chord)->
    receive
        {lookup, Key} ->io:format("where is this key"),
        Num=Chord#chord.numrequests,
        case Num of
            0->%SEND MESSAGE TO TRACKER TO STOP;
                chordNode(Chord);
            _->
                NewChord=Chord#chord{numrequests=Num-1},
                self() ! {update, NewChord},
                chordNode(Chord)         
        end;
        {update, UpdateChord}->
            chordNode(UpdateChord);
        {terminate} -> io:format("received signal to stop");
        print->
            printNode(Chord),
            chordNode(Chord)
    end,
    ok.

stop()->
    exit(self(),kill).

print(Pid)->
    Pid ! print,
    ok.

%%ACTOR TO KEEP TRACK OF FINISHING ACTORS
%%TRACKER FUNCTIONS
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
                io:format("All finished, need to avg calculation"),
                sendTerminate(NumActors, L);
            _->track(State)
        end
    end.

sendTerminate(0,_)->
    ok;

sendTerminate(NumActors,List)->
    lists:nth(NumActors-1,List) ! terminate,
    sendTerminate(NumActors-1, lists:nth(NumActors-2,List)).

startTrack()->
    statistics(wall_clock),
    Set = sets:new(),
    State= #tracker{numNodes=0, finishedNodes=Set},
    ActorPid = spawn_link(?MODULE, track, [State]),
    register(list_to_atom("chord_tracker"), ActorPid).
