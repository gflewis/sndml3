package sndml.datamart;

public enum Action {
	INSERT, 
	UPDATE, 
	SYNC, 
	PRUNE, 
	EXECUTE, // Execute an SQL command
	CREATE,  // Create a table
	DROPTABLE, // Used for JUnit tests
	LOAD,    // Alias for INSERT 
	REFRESH  // Alias for UPSERT
	}
