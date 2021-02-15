package sndml.datamart;

public enum Action {
	INSERT, 
	UPDATE, 
	SYNC, 
	PRUNE, 
	CREATE, 
	LOAD,    // Alias for INSERT 
	REFRESH  // Alias for UPSERT
	}
