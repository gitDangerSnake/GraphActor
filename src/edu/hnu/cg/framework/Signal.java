package edu.hnu.cg.framework;

public interface Signal {
	
	/// manager ----- > worker
	int ACTIVE = 0 ; 
	int HALT = 42 ; 
	int INACTIVE= 23;
	
	// worker ------ > manager
	int DONE = 1;
	int UPDATED = 2;
	int FREE = 3;
	
	// manager ----- > loader  
	int LOAD = 4;
	int HAS_CAPACITY = 5;
	int NO_AVAILABLE_SPACE = 6;
	
	//loader ------ > manager
	int LOAD_HAPPENED = 7;
	
	//manager ----- > assigner
	int DISTRIBUTE = 10 ; 
	
	
	// manager ----- > unloader
	int UNLOAD = 5;
	
	
	//unloader ------ > manager
	int UNLOAD_HAPPENED = 7;
}
