package sndml.loader;

import java.util.EnumSet;

import com.fasterxml.jackson.annotation.JsonFormat;

@JsonFormat(with = JsonFormat.Feature.ACCEPT_CASE_INSENSITIVE_VALUES)
public enum Action {
	/**
	 * Insert new records and generate a warning if a primary key violation occurs.
	 */
	INSERT,
	
	/**
	 * Insert new records and update existing records.
	 */
	UPDATE,
	
	/**
	 * Compare all timestampls and insert, update or delete records accordingly.
	 */
	SYNC,
	
	/**
	 * Delete records based on sys_audit_delete. 
	 */
	PRUNE, 
	
	/**
	 * Execute an SQL command.
	 */
	EXECUTE,
	
	/**
	 * Synchronize a single record
	 */
	ROWSYNC,

	/**
	 * Create a table.
	 */
	CREATE,
	
	/**
	 * Drop a table. Used for JUnit tests.
	 */
	DROPTABLE,
	
	/**
	 * Alias for INSERT.
	 */
	LOAD,
	
	/**
	 * Alias for UPDATE.
	 */
	REFRESH;
	
	public static EnumSet<Action> INSERT_UPDATE =
			EnumSet.of(INSERT, UPDATE, LOAD, REFRESH);
	
	public static EnumSet<Action> INSERT_UPDATE_SYNC =
			EnumSet.of(INSERT, UPDATE, SYNC, LOAD, REFRESH);

	public static EnumSet<Action> INSERT_UPDATE_PRUNE =
			EnumSet.of(INSERT, UPDATE, PRUNE, LOAD, REFRESH);

	public static EnumSet<Action> ANY_TABLE_ACTION =
			EnumSet.of(INSERT, UPDATE, SYNC, PRUNE, CREATE, DROPTABLE, LOAD, REFRESH, ROWSYNC);

	public static EnumSet<Action> EXECUTE_ONLY =
			EnumSet.of(EXECUTE);
	
	public static EnumSet<Action> SINGLE_ONLY =
			EnumSet.of(ROWSYNC);
	
}