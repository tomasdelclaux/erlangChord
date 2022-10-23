-module(chord).
-compile(export_all).
-author("Tomas Delclaux Rodriguez-Rey and Ariel Weitzenfeld").
-record(chord, {id, finger, successor, predecessor, key, numrequests}).
-record(tracker, {numNodes, finishedNodes}).
-define(M, 160).

% getNext(FingerTable)->
%     %%TODO

%% join()->
    %%TODO


%% need to implement stabilise too


init()->
    {ok,[NumNodes, NumRequests]} = 
        io:fread("Enter number of chord nodes and number of requests", "~d~d"),
    ok.

%% CHORD IMPLEMENTATION FUNCTIONS

% FINGER TABLE
create_finger_table(_,0)->
    [];

create_finger_table(ID,M)->
    [ID|create_finger_table(ID,M-1)].

finger_print([])-> 
    [];

finger_print([H|T]) ->
    io:format("    ~p~n", [H]),
    [H|finger_print(T)].

% HASHING
getHash(Data) ->  
    binary:decode_unsigned(crypto:hash(sha, Data)).

%% CHORD NODE API

%CREATE A NODE AND A NETWORK WITH THE GIVEN NUMBER OF REQUESTS
create(NumRequests)->
    Chord=#chord{},
    NodePid = spawn_link(?MODULE, chordNode, [Chord]),
    Id=getHash(pid_to_list(NodePid)),
    ID=list_to_atom(integer_to_list(Id)),
    register(ID, NodePid),
    Finger=create_finger_table(ID,?M),
    Key=getHash("1"),
    String="I am node 1",
    Map=#{Key=> String},
    UpdateChord = #chord{id=ID, finger=Finger, successor=ID, key=Map, numrequests=NumRequests},
    NodePid ! {update, UpdateChord},
    {ok, ID}.

%JOIN FUNCTION - NEW NODE IS N and NPRIME IS A NODE IN THE NETWORK
join(NPrime, NumRequests, NodeNum)->
    Chord=#chord{},
    NodePid = spawn_link(?MODULE, chordNode, [Chord]),
    Id=getHash(pid_to_list(NodePid)),
    ID=list_to_atom(integer_to_list(Id)),
    register(ID, NodePid),
    Finger=create_finger_table(ID,?M),
    Key=getHash(integer_to_list(NodeNum)),
    String="I am node "++integer_to_list(NodeNum),
    Map=#{Key=> String},
    UpdateChord = #chord{id=ID, finger=Finger, successor=ID, key=Map, numrequests=NumRequests},
    NodePid ! {update, UpdateChord},
    NodePid ! {join, NPrime}.
    

%FIND SUCCESSOR
find_successor(State, ID)->
    io:format("find successor~n"),
    N=State#chord.id,
    Successor=State#chord.successor,
    if
        (ID>N) and (ID=<Successor)-> Successor;
        true->
            NPrime=closest_preceding_node(State, ID),
            io:format("Nprime ~w with ~w", [NPrime, whereis(NPrime)]),
            if 
                NPrime == N -> N;
                true-> 
                    whereis(NPrime) ! {find_successor, ID},
                    receive
                        {successor, Successor}->Successor
                    end
            end
    end.

closest_preceding_node(State, ID)->
    io:format("search table~n"),
    N=State#chord.id,
    Finger=State#chord.finger,
    search_table(?M, N, ID, Finger).

search_table(0,N,_,_)->
    io:format("Ending search table ~w~n", [N]),
    N;

search_table(I, N, ID, Finger)->
    Nth=lists:nth(I, Finger),
    if
        (Nth>N) and (Nth<ID)->Nth;
        true->
            search_table(I-1, N, ID, Finger)
    end.

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
    io:format("RUNNING~n"),
    receive
        {lookup, Key} ->io:format("where is this key"),
        Num=Chord#chord.numrequests,
        case Num of
            0->ok;%SEND MESSAGE TO TRACKER TO STOP;
            _->
                NewChord=Chord#chord{numrequests=Num-1},
                self() ! {update, NewChord}
        end;
        {find_successor, ID}->
            io:format("received message to get successor~n"),
            whereis(ID) ! {successor, find_successor(Chord, ID)};
        {successor, Successor}->
            io:format("received successor"),
            UpdateChord=Chord#chord{successor=Successor},
            chordNode(UpdateChord);
        {join, N}->
            whereis(N) ! {find_successor, Chord#chord.id};
        {update, UpdateChord}->
            chordNode(UpdateChord);
        {terminate} -> io:format("received signal to stop");
        print->
            printNode(Chord);
        _->
            io:format("received rubbish")
    end,
    chordNode(Chord).

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
