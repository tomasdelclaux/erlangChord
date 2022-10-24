-module(chord).
-compile(export_all).
-author("Tomas Delclaux Rodriguez-Rey and Ariel Weitzenfeld").
-record(chord, {id, finger, successor, predecessor, key, numrequests, next, numNodes}).
-record(tracker, {numNodes, finishedNodes, totalHops, totalRequests, maxRequests}).
-define(M, 160).
-define(STABILIZE_TIME,50).
-define(KEY_FIX_TIME,5000).

%% MAIN PROGRAM
insert_data(0,FirstNode)->
    insert(FirstNode,0),
    ok;

insert_data(NumNodes,FirstNode)->
    insert(FirstNode, NumNodes),
    insert_data(NumNodes-1, FirstNode),
    ok.

add_nodes(0,_,_,_)->
    ok;

add_nodes(NumNodes, FirstNode, NumRequests, MaxNodes)->
    join(FirstNode, NumRequests, MaxNodes),
    add_nodes(NumNodes-1, FirstNode, NumRequests, MaxNodes),
    % search(FirstNode, NumNodes),
    ok.

init()->
    {ok,[NumNodes, NumRequests]} = io:fread("", "~d~d"),
    startTrack(NumNodes, NumRequests),
    {ok,FirstNode}=create(0, NumNodes),
    add_nodes(NumNodes-1, FirstNode, 0, NumNodes),
    timer:sleep(round(NumNodes*1000)),
    insert_data(NumNodes,FirstNode),
    ok.

%% CHORD IMPLEMENTATION FUNCTIONS

% FINGER TABLE
create_finger_table(0)->
    [];

%% INITIALIZE TO VALUE 2^160, which is larger than the largest hash of SHA-1
create_finger_table(M)->
    ['1461501637330902918203684832716283019655932542976'|create_finger_table(M-1)].

finger_print([], _,_)-> 
    [];

finger_print([H|T],ID, M) ->
    Id=list_to_integer(atom_to_list(ID)),
    Index=(Id+round(math:pow(2,M-1))) rem round(math:pow(2, ?M)),
    io:format("    Index:~p~p~n", [Index,H]),
    [H|finger_print(T, ID, M+1)].

% HASHING
getHash(Data) ->  
    binary:decode_unsigned(crypto:hash(sha, Data)).

%%USER FUNCTIONS
insert(Node,Data)->
    String="I am node "++integer_to_list(Data),
    Key=list_to_atom(integer_to_list(getHash(integer_to_list(Data)))),
    whereis(Node) ! {find_insert_point, Key, Node, String},
    ok.

search(Node, Data)->
    Key=list_to_atom(integer_to_list(getHash(integer_to_list(Data)))),
    Counter=0,
    whereis(Node) ! {search_key, Key, Node, Counter},
    ok.

%% CHORD NODE API
create(NumRequests,NumNodes)->
    Chord=#chord{},
    Finger=create_finger_table(?M),
    NodePid = spawn_link(?MODULE, chordNode, [Chord]),
    Id=getHash(pid_to_list(NodePid)),
    ID=list_to_atom(integer_to_list(Id)),
    register(ID, NodePid),
    Key=maps:new(),
    UpdateChord = #chord{id=ID, finger=Finger, successor=ID, predecessor=nill, key=Key, numrequests=NumRequests, next=?M, numNodes=NumNodes},
    NodePid ! {update, UpdateChord},
    {ok, ID}.

%JOIN FUNCTION - NEW NODE IS N and NPRIME IS A NODE IN THE NETWORK
join(NPrime, NumRequests, NumNodes)->
    Chord=#chord{},
    Finger=create_finger_table(?M),
    NodePid = spawn_link(?MODULE, chordNode, [Chord]),
    Id=getHash(pid_to_list(NodePid)),
    ID=list_to_atom(integer_to_list(Id)),
    register(ID, NodePid),
    Key=maps:new(),
    UpdateChord = #chord{id=ID, finger=Finger, successor=ID, predecessor=nill, key=Key, numrequests=NumRequests, next=?M, numNodes=NumNodes},
    NodePid ! {update, UpdateChord},
    NodePid ! {join, NPrime},
    {ok, ID}.

closest_preceding_node(State, ID)->
    N=State#chord.id,
    Finger=State#chord.finger,
    search_table(?M, N, ID, Finger).

search_table(0,N,_,_)->
    N;

search_table(I, N, ID, Finger)->
    Nth=lists:nth(I, Finger),
    X=atom_to_int(Nth),
    Y=atom_to_int(ID),
    Z=atom_to_int(N),
    if
        (Y>Z) and (X>Z) and (X<Y)->Nth;
        (Y<Z) and ((X<Y) and (Y<Z))->Nth;
        true->
            search_table(I-1, N, ID, Finger)
    end.

printNode(Chord)->
    io:format("OUTPUTTING STATE OF NODE:\n"),
    io:format("ID: ~w~n", [Chord#chord.id]),
    io:format("Finger~n"),
    finger_print(Chord#chord.finger, Chord#chord.id, 1),
    io:format("Predecessor: ~w~n", [Chord#chord.predecessor]),
    io:format("Successor: ~w~n", [Chord#chord.successor]),
    io:format(lists:flatten(io_lib:format("~p", [maps:to_list(Chord#chord.key)]))),
    io:format("~nnumRequests: ~w~n", [Chord#chord.numrequests]).

chordNode(Chord)->
    receive
        {find_successor, ID, Node, Reply, Next}->
            case Reply of
                search_point->
                    {Key, Counter}=Next,
                    NewCounter=Counter+1,
                    NewNext={Key,NewCounter};
                _->NewNext=Next
            end,
            N=Chord#chord.id,
            Successor=Chord#chord.successor,
            X=atom_to_int(N),
            Y=atom_to_int(Successor),
            Z=atom_to_int(ID),
            if
                (X>Y) and ((Z>X) or (Z=<Y))-> whereis(Node) ! {Reply, Successor, NewNext};
                (X<Y) and (Z>X) and (Z=<Y)-> whereis(Node) ! {Reply, Successor, NewNext};
                true->
                    NPrime=closest_preceding_node(Chord, ID),
                    if 
                        NPrime == N ->
                            whereis(Node) ! {Reply, NPrime, NewNext};
                        true->
                            whereis(NPrime) ! {find_successor, ID, Node, Reply, NewNext}
                    end
            end;          
        {getSuccessor, Successor, _}->
            UpdateChord=Chord#chord{successor=Successor},
            chordNode(UpdateChord);
        {fix_key, Successor, Next}->
            UpdateChord=update_key_entry(Chord,Successor,Next),
            chordNode(UpdateChord);
        {join, N}->whereis(N) ! {find_successor, Chord#chord.id, Chord#chord.id, getSuccessor, 0};
        {check_predecessor, ID}->
            whereis(ID) ! {stabilize,Chord#chord.predecessor};
        {stabilize, ID}->
            if
                ID==nill->notify(Chord#chord.successor,Chord#chord.id);
                true->
                    X=atom_to_int(Chord#chord.successor),
                    Y=atom_to_int(ID),
                    Z=atom_to_int(Chord#chord.id),
                    if
                        (Chord#chord.id==Chord#chord.successor)->
                            UpdateChord=Chord#chord{successor=ID},
                            notify(ID,Chord#chord.id),
                            chordNode(UpdateChord);
                        (Z<X) and (Y>Z) and (Y<X)->
                            UpdateChord=Chord#chord{successor=ID},
                            notify(ID,Chord#chord.id),
                            chordNode(UpdateChord);
                        (Z>X) and ((Y>Z) or (Y<X))->
                            UpdateChord=Chord#chord{successor=ID},
                            notify(ID,Chord#chord.id),
                            chordNode(UpdateChord);
                        true->notify(Chord#chord.successor,Chord#chord.id)
                    end
            end;
        {notify, ID}->
            X=atom_to_int(Chord#chord.predecessor),
            Y=atom_to_int(ID),
            Z=atom_to_int(Chord#chord.id),
            if
                (Chord#chord.predecessor==nill)->
                    Pred1 = fun(K,V) -> 
                        K_int = atom_to_int(K),
                        if 
                            K_int > Z ->
                                true;
                            true ->
                                K_int=<Y
                        end
                    end,
                    Pred2 = fun(K,V) -> 
                        K_int = atom_to_int(K),
                        if 
                            K_int > Z ->
                                false;
                            true ->
                                K_int>Y
                        end
                    end,
                    whereis(ID) ! {update_keys, maps:filter(Pred1,Chord#chord.key)},
                    UpdateChord=Chord#chord{predecessor=ID, key=maps:filter(Pred2,Chord#chord.key)},
                    %find keys smaller than new predecessor (adjusted to circle), send them and then remove them from me
                    chordNode(UpdateChord);
                (Z>X) and (Y>X) and (Y<Z)->
                    UpdateChord=Chord#chord{predecessor=ID},
                    chordNode(UpdateChord);
                (Z<X) and ((Y>X) or (Y<Z))->
                    UpdateChord=Chord#chord{predecessor=ID},
                    chordNode(UpdateChord);
                true->ok
            end;
        {update, UpdateChord}-> 
            chordNode(UpdateChord);
        {update_keys, Keys}-> 
            UpdateChord=Chord#chord{key=maps:merge(Keys, Chord#chord.key)},
            chordNode(UpdateChord);
        {find_insert_point, Key, Node, Data}->
            self() ! {find_successor, Key, Node, insert_point, {Key, Data}};
        {search_key, Key, Node, Counter}->
            self() ! {find_successor, Key, Node, search_point, {Key,Counter}};
        {search_point, Successor, {Key,Counter}}->
            whereis(Successor) ! {fetch_key, {Key, Counter}};
        {fetch_key, {Key,Counter}}->
            % whereis(chord_tracker) ! {hops, {Chord#chord.id, Counter, Chord#chord.numrequests+1}},
            UpdateChord=Chord#chord{numrequests=Chord#chord.numrequests+1},
            Res=maps:find(Key, Chord#chord.key),
            case Res of
                error->io:format("KEY NOT FOUND ~w~n",[Key]);
                {ok, V}->io:format("KEY FOUND AT NODE ~w with DATA:~s~n",[Chord#chord.id, V]);
                _->io:format("ERROR~n")
            end,
            chordNode(UpdateChord);
        {insert_point, Successor, {Key, Data}}->
            whereis(Successor) ! {insert, {Key, Data}};
        {insert, {Key, Data}}->
            UpdateChord=Chord#chord{key=maps:put(Key,Data,Chord#chord.key)},
            chordNode(UpdateChord);
        print->printNode(Chord);
        terminate -> io:format("received signal to stop")
        after ?STABILIZE_TIME ->
            whereis(Chord#chord.successor) ! {check_predecessor, Chord#chord.id},
            fix_fingers(Chord)
            % search(Chord#chord.id, rand:uniform(Chord#chord.numNodes))
    end,
    chordNode(Chord).

notify(Successor, Me)->
    if
        Successor==Me->ok;
        true->
            whereis(Successor) ! {notify, Me}
    end.

fix_fingers(State)->
    if
        State#chord.next+1>?M->Next=1;
        true->Next=State#chord.next+1
    end,
    ID=(list_to_integer(atom_to_list(State#chord.id))+round(math:pow(2, Next-1))) rem round(math:pow(2, ?M)),
    self() ! {find_successor, list_to_atom(integer_to_list(ID)), State#chord.id, fix_key, Next}.

update_key_entry(State,Successor,Next)->
    Finger=State#chord.finger,
    NewFinger=lists:sublist(Finger,Next-1) ++ [Successor] ++ lists:nthtail(Next,Finger),
    NewState=State#chord{finger=NewFinger, next=Next},
    NewState.

stop()->
    exit(self(),kill).

print(Pid)->
    Pid ! print,
    ok.

atom_to_int(Atom)->
    case Atom of
        nill->nill;
        _->list_to_integer(atom_to_list(Atom))
    end.


%%ACTOR TO KEEP TRACK OF FINISHING ACTORS
%%TRACKER FUNCTIONS
track(State)->
    receive
        {hops, {ID, Counter, NumRequests}}->
            Hops= State#tracker.totalHops+Counter,
            Reqs= State#tracker.totalRequests+1,
            MaxReq=State#tracker.maxRequests,
            case NumRequests of
                MaxReq->
                    S2 = sets:add_element(ID, State#tracker.finishedNodes),
                    NewTrack=State#tracker{finishedNodes=S2,totalHops=Hops, totalRequests=Reqs},
                    track(NewTrack);
                _->NewTrack=State#tracker{totalHops=Hops, totalRequests=Reqs},
                 track(NewTrack)
            end
    after 10 ->
        L = sets:to_list(State#tracker.finishedNodes),
        NumActors=State#tracker.numNodes,
        case length(L) of
            NumActors -> 
                io:format("AVERAGE HOPS PER SEARCH ~w~n", [State#tracker.totalHops/State#tracker.totalRequests]);
                % sendTerminate(L);
            _->track(State)
        end
    end.

sendTerminate([])->
    ok;

sendTerminate([H|T])->
    whereis(H) ! terminate,
    sendTerminate(T).

startTrack(NumNodes, NumRequests)->
    statistics(wall_clock),
    Set = sets:new(),
    State= #tracker{numNodes=NumNodes, maxRequests=NumRequests, finishedNodes=Set, totalHops=0, totalRequests=0},
    ActorPid = spawn_link(?MODULE, track, [State]),
    register(list_to_atom("chord_tracker"), ActorPid).

