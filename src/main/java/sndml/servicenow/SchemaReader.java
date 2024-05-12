package sndml.servicenow;

import java.io.IOException;

/**
 * An abstract class used to read table definitions from the ServiceNow instance. 
 * There are two implementations: {@link sndml.servicenow.TableSchemaReader} 
 * and {@link sndml.agent.AppSchemaReader}.
 */
public interface SchemaReader {

	TableSchema getSchema(String tablename) throws IOException, InterruptedException;

}
