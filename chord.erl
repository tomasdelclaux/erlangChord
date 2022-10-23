-module(chord).
-compile(export_all).
-author("Tomas Delclaux Rodriguez-Rey and Ariel Weitzenfeld").
-record(chord, {id, finger, successor, predecessor, key, numrequests, next}).
-record(tracker, {numNodes, finishedNodes}).
-define(M, 7).
-define(STABILIZE_TIME,2000).
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
    [0|create_finger_table(M-1)].

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
    Finger=create_finger_table(?M),
    Key=getHash("1"),
    String="I am node 1",
    Map=#{Key=> String},
    UpdateChord = #chord{id=ID, finger=Finger, successor=ID, predecessor=nill, key=Map, numrequests=NumRequests, next=?M},
    NodePid ! {update, UpdateChord},
    {ok, ID}.

%JOIN FUNCTION - NEW NODE IS N and NPRIME IS A NODE IN THE NETWORK
join(NPrime, NumRequests, NodeNum)->
    Chord=#chord{},
    NodePid = spawn_link(?MODULE, chordNode, [Chord]),
    Id=getHash(pid_to_list(NodePid)),
    ID=list_to_atom(integer_to_list(Id)),
    register(ID, NodePid),
    Finger=create_finger_table(?M),
    Key=getHash(integer_to_list(NodeNum)),
    String="I am node "++integer_to_list(NodeNum),
    Map=#{Key=> String},
    UpdateChord = #chord{id=ID, finger=Finger, successor=ID, predecessor=nill, key=Map, numrequests=NumRequests, next=?M},
    NodePid ! {update, UpdateChord},
    NodePid ! {join, NPrime},
    {ok, ID}.

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
    receive
        {find_successor, ID, Node, Reply, Next}->
            io:format("received message to get successor~n"),
            N=Chord#chord.id,
            Successor=Chord#chord.successor,
            io:format("ID ~w, N ~w, Successor ~w", [ID,N,Successor]),
            if
                (ID>N) and (ID=<Successor)-> whereis(Node) ! {Reply, Successor, Next};
                true->
                    NPrime=closest_preceding_node(Chord, ID),
                    if 
                        NPrime == N ->whereis(Node) ! {Reply, NPrime, Next};
                        true-> whereis(NPrime) ! {find_successor, ID, Node, Reply, Next}
                    end
            end;            
        {getSuccessor, Successor}->
            io:format("received successor~n"),
            UpdateChord=Chord#chord{successor=Successor},
            chordNode(UpdateChord);
        {fix_key, Successor, Next}->
            UpdateChord=update_key_entry(Chord,Successor,Next),
            chordNode(UpdateChord);
        {join, N}->whereis(N) ! {find_successor, Chord#chord.id, Chord#chord.id, getSuccessor, 0};
        {stabilize, ID}->
            % io:format("STABILIZATION FROM ~w~n", [ID]),
            whereis(ID) ! {check_predecessor,Chord#chord.predecessor};
        {check_predecessor, ID}->
            if
                ID==nill->notify(Chord#chord.successor,Chord#chord.id);
                true->
                    if
                        (Chord#chord.id==Chord#chord.successor)->
                            UpdateChord=Chord#chord{successor=ID},
                            notify(Chord#chord.successor,Chord#chord.id),
                            chordNode(UpdateChord);
                        (ID>Chord#chord.id) and (ID<Chord#chord.successor)->
                            UpdateChord=Chord#chord{successor=ID},
                            notify(Chord#chord.successor,Chord#chord.id),
                            chordNode(UpdateChord);
                        true->notify(Chord#chord.successor,Chord#chord.id)
                    end
            end;
        {notify, ID}->
            % io:format("NOTIFICATION FROM ~w~n", [ID]),
            if
                (Chord#chord.predecessor==nill)or((ID>Chord#chord.predecessor) and (ID<Chord#chord.id))->
                    UpdateChord=Chord#chord{predecessor=ID},
                    chordNode(UpdateChord);
                true->ok
            end;
        {update, UpdateChord}->chordNode(UpdateChord);
        {terminate} -> io:format("received signal to stop");
        print->printNode(Chord)
        after ?STABILIZE_TIME ->
            whereis(Chord#chord.successor) ! {stabilize, Chord#chord.id},
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
    ID=list_to_integer(atom_to_list(State#chord.id))+math:pow(2, Next-1),
    self() ! {find_successor, ID, State#chord.id, fix_key, Next}.

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