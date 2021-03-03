package sndml.datamart;

import java.util.EnumSet;

public enum Action {
	INSERT, 
	UPDATE, 
	SYNC, 
	PRUNE, 
	EXECUTE, // Execute an SQL command
	CREATE,  // Create a table
	DROPTABLE, // Used for JUnit tests
	LOAD,    // Alias for INSERT 
	REFRESH;  // Alias for UPSERT
	
	public static EnumSet<Action> anyLoadAction =
			EnumSet.of(INSERT, UPDATE, SYNC, LOAD, REFRESH);

	public static EnumSet<Action> anyTableAction =
			EnumSet.of(INSERT, UPDATE, SYNC, PRUNE, CREATE, DROPTABLE, LOAD, REFRESH);

	public static EnumSet<Action> anyGlobalAction =
			EnumSet.of(EXECUTE);
	
}