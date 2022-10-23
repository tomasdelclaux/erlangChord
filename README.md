# erlangChord

## AUTHORS
Ariel Weitzenfeld and Tomas Delclaux

## INTRODUCTION
An implementation using erlang of the Chord algorithm for Distributed Hash Tables

Our implementation uses the idea of a circular set of nodes to allow for searches and connections to be made from any node in the system. This is not explicitly outlind in the given resources regarding the chord API, but we extend the algorithms to invlove the idea of circularity. A node near the end of the number of nodes may have its finger table wrap into the first nodes, allowing for an even better and more complete search algorithm. 

## RUNNING IT
TODO

## WHAT IS WORKING?
Everything.

## LARGEST NETWORK?
TODO
