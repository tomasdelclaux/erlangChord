-module(chord).
-compile(export_all).
-author("Tomas Delclaux Rodriguez-Rey and Ariel Weitzenfeld").
-record(chord, {id, finger, successor, predecessor, key, numrequests, next}).
-record(tracker, {numNodes, finishedNodes}).
-define(M, 160).
-define(STABILIZE_TIME,50).
-define(KEY_FIX_TIME,5000).

%% MAIN PROGRAM

init()->
    {ok,[NumNodes, NumRequests]} = 
        io:fread("Enter number of chord nodes and number of requests", "~d~d"),
    ok.

%% CHORD IMPLEMENTATION FUNCTIONS

% FINGER TABLE
create_finger_table(0)->
    [];

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
    whereis(Node) ! {find_insert_point, Key, Node, String}.

search(Node, Data)->
    Key=list_to_atom(integer_to_list(getHash(integer_to_list(Data)))),
    whereis(Node) ! {search_key, Key, Node}.

%% CHORD NODE API

%CREATE A NODE AND A NETWORK WITH THE GIVEN NUMBER OF REQUESTS
create(NumRequests)->
    Chord=#chord{},
    Finger=create_finger_table(?M),
    NodePid = spawn_link(?MODULE, chordNode, [Chord]),
    Id=getHash(pid_to_list(NodePid)),
    ID=list_to_atom(integer_to_list(Id)),
    register(ID, NodePid),
    Key=maps:new(),
    UpdateChord = #chord{id=ID, finger=Finger, successor=ID, predecessor=nill, key=Key, numrequests=NumRequests, next=?M},
    NodePid ! {update, UpdateChord},
    {ok, ID}.

%JOIN FUNCTION - NEW NODE IS N and NPRIME IS A NODE IN THE NETWORK
join(NPrime, NumRequests)->
    Chord=#chord{},
    Finger=create_finger_table(?M),
    NodePid = spawn_link(?MODULE, chordNode, [Chord]),
    Id=getHash(pid_to_list(NodePid)),
    ID=list_to_atom(integer_to_list(Id)),
    register(ID, NodePid),
    Key=maps:new(),
    UpdateChord = #chord{id=ID, finger=Finger, successor=ID, predecessor=nill, key=Key, numrequests=NumRequests, next=?M},
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
        (Y<Z) and (X<Y) and (Y<Z)->Nth;
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
            N=Chord#chord.id,
            Successor=Chord#chord.successor,
            X=atom_to_int(N),
            Y=atom_to_int(Successor),
            Z=atom_to_int(ID),
            if
                (X>Y) and ((Z>X) or (Z=<Y))-> whereis(Node) ! {Reply, Successor, Next};
                (X<Y) and (Z>X) and (Z=<Y)-> whereis(Node) ! {Reply, Successor, Next};
                true->
                    NPrime=closest_preceding_node(Chord, ID),
                    if 
                        NPrime == N ->
                            whereis(Node) ! {Reply, NPrime, Next};
                        true->
                            whereis(NPrime) ! {find_successor, ID, Node, Reply, Next}
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
            % if
            %     (ID=='684329801336223661356952546078269889038938702779')->
            %         io:format("NOTIFY MESSAGE I am ~w, Pre is ~w~n", [Chord#chord.id, Chord#chord.predecessor]);
            %     true->ok
            % end,
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
        {terminate} -> io:format("received signal to stop");
        {find_insert_point, Key, Node, Data}->
            self() ! {find_successor, Key, Node, insert_point, {Key, Data}};
        {search_key, Key, Node}->
            self() ! {find_successor, Key, Node, search_point, Key};
        {search_point, Successor, Key}->
            whereis(Successor) ! {fetch_key, Key};
        {fetch_key, Key}->
            io:format("KEY FOUND AT NODE ~w with DATA:~s",[Chord#chord.id, maps:get(Key, Chord#chord.key)]);
        {insert_point, Successor, {Key, Data}}->
            whereis(Successor) ! {insert, {Key, Data}};
        {insert, {Key, Data}}->
            io:format("INSERTED"),
            UpdateChord=Chord#chord{key=maps:put(Key,Data,Chord#chord.key)},
            chordNode(UpdateChord);
        print->printNode(Chord)
        after ?STABILIZE_TIME ->
            whereis(Chord#chord.successor) ! {check_predecessor, Chord#chord.id},
            fix_fingers(Chord)
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





%% FIND SUCCESSOR
% find_successor(State, ID)->
%     N=State#chord.id,
%     Successor=State#chord.successor,
%     if
%         (ID>N) and (ID=<Successor)-> Successor;
%         true->
%             NPrime=closest_preceding_node(State, ID),
%             io:format("Nprime ~w with ~w", [NPrime, whereis(NPrime)]),
%             if 
%                 NPrime == N -> N;
%                 true-> 
%                     whereis(NPrime) ! {find_successor, ID},
%                     receive
%                         {successor, Successor}->Successor
%                     end
%             end
%     end.