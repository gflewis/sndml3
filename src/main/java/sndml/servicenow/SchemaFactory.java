package sndml.servicenow;

import java.io.IOException;

public abstract class SchemaFactory {

	abstract public TableSchema getSchema(String tablename) 
		throws IOException, InterruptedException;
	
}
