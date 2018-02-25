# ra-operators
Fundamental RA operators implemented in java

This project is actually a series of projects done as part of Database Management Systems (CSCI 6370) @ UGA. We in team of 
five students implemented fundamental Database operators like select, project, join, union and minus. Later, Indexing was also
added with data structures like Bplus tree and Linear Hashmap. 

Note: Some of the files were provided by course instructor.

I have worked exclusively on join operation and indexing on Linear Hashmap data structure.

## Description of project

1. Finish the implementation the 5 RA Operators that are partially implemented in Table.java. Store tuples in an ArrayList. Use    MapType.NO_MAP for the index type (no indexing).

2. Use indices (TreeMap (in Java), LinHashMap and BpTreeMap) to speed up the processing of Select and Join. Indices must be        integrated and used in the Table class. For a bonus of up to (+10) provide the option of storing the tuples in a FileList as    well as in an ArrayList.

## How to run
Compile this program using sbt (simple build tool). While in the project directory, do the follow:
           	 
		 - sbt clean
	   	 - sbt compile run
	    
You will be prompted to select a main function. 
	    	  
		 - MovieDB.java
		 - LinHashMap.java
		 - BpTreeMap.java
		 - KeyType.java

Their respective main methods test their class' various functionality.
