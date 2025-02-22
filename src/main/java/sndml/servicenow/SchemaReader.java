package sndml.servicenow;

import java.io.IOException;

/**
 * <p>An abstract class used to read table definitions from the ServiceNow instance. 
 * There are two implementations:</p> 
 * <ul>
 * <li>{@link sndml.servicenow.TableSchemaReader}</li> 
 * <li>{@link sndml.agent.AppSchemaReader}</li>
 * </ul>
 */
public interface SchemaReader {

	TableSchema getSchema(String tablename) throws IOException, InterruptedException;
	
	TableSchema getSchema(Table table) throws IOException, InterruptedException;

}
