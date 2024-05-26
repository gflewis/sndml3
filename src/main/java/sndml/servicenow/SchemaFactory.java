package sndml.servicenow;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

import sndml.util.Log;

public class SchemaFactory {

	private final SchemaReader schemaReader;
	
	private final static ConcurrentHashMap<String,TableSchema> schemaCache = 
			new ConcurrentHashMap<String,TableSchema>();

	public SchemaFactory(SchemaReader r) {
		this.schemaReader = r;
	}
	
//	public static void setSchemaReader(SchemaReader r) {
//		schemaReader = r;
//	}
	
//	public static SchemaReader getSchemaReader() {
//		assert schemaReader != null;
//		return schemaReader;
//	}

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
