package sndml.datamart;

import java.util.EnumSet;

public enum Action {
	INSERT, 
	UPDATE, 
	SYNC, 
	PRUNE, 
	EXECUTE, // Execute an SQL command
	SINGLE, // Synchronize a single record
	CREATE,  // Create a table
	DROPTABLE, // Used for JUnit tests
	LOAD,    // Alias for INSERT 
	REFRESH;  // Alias for UPSERT
	
	public static EnumSet<Action> INSERT_UPDATE =
			EnumSet.of(INSERT, UPDATE, LOAD, REFRESH);
	
	public static EnumSet<Action> INSERT_UPDATE_SYNC =
			EnumSet.of(INSERT, UPDATE, SYNC, LOAD, REFRESH);

	public static EnumSet<Action> INSERT_UPDATE_PRUNE =
			EnumSet.of(INSERT, UPDATE, PRUNE, LOAD, REFRESH);

	public static EnumSet<Action> ANY_TABLE_ACTION =
			EnumSet.of(INSERT, UPDATE, SYNC, PRUNE, CREATE, DROPTABLE, LOAD, REFRESH, SINGLE);

	public static EnumSet<Action> EXECUTE_ONLY =
			EnumSet.of(EXECUTE);
	
	public static EnumSet<Action> SINGLE_ONLY =
			EnumSet.of(SINGLE);
	
}