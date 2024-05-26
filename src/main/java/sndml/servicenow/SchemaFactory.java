package sndml.servicenow;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

import sndml.util.Log;

/**
 * A class that maintains a cache of {@link TableSchema} objects that have been read 
 * from the instance, and reuses them if they are required a second time.
 *
 */
public class SchemaFactory {

	private final SchemaReader schemaReader;
	
	private final static ConcurrentHashMap<String,TableSchema> schemaCache = 
			new ConcurrentHashMap<String,TableSchema>();

	public SchemaFactory(SchemaReader r) {
		this.schemaReader = r;
	}
	
	/**
	 * Generate {@link TableSchema} or retrieve from cache.
	 */	
	public TableSchema getSchema(String tablename) throws IOException, InterruptedException {
		if (schemaCache.containsKey(tablename)) 
			return schemaCache.get(tablename);
		assert schemaReader != null : SchemaFactory.class.getSimpleName() + " not initialized";
		String saveJob = Log.getJobContext();
		Log.setJobContext(tablename + ".schema");		
		TableSchema schema = schemaReader.getSchema(tablename);
		if (schema.isEmpty()) throw new InvalidTableNameException(tablename);
		schemaCache.put(tablename, schema);
		Log.setJobContext(saveJob);
		return schema;
	}

}
