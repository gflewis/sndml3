package sndml.servicenow;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableSchemaFactory extends SchemaFactory {

	private final Session session;
	private final Table dictionary;
	private final Table hierarchy;
	
	final private Logger logger = LoggerFactory.getLogger(this.getClass());
	
	public TableSchemaFactory(Session session) {
		this.session = session;
		this.dictionary = session.table("sys_dictionary");
		this.hierarchy = session.table("sys_db_object");		
	}
	
	@Override
	public TableSchema getSchema(String tablename) throws IOException, InterruptedException {
		return getSchema(session.table(tablename));
	}
	
	TableSchema getSchema(Table table) throws IOException, InterruptedException {
		String tablename = table.getName();
		String saveJob = Log.getJobContext();
		String myname = dictionary.getName() + "." + tablename;
		Log.setTableContext(dictionary,  myname);		
		TableSchema schema = new TableSchema(table);
		logger.debug(Log.SCHEMA, "get definition for table " + tablename);
		String parentname = determineParentName(tablename);
		logger.debug(Log.SCHEMA, tablename + " parent is " + parentname);
		if (parentname != null) {
			// recursive call for parent definition
			TableSchema parentSchema = getSchema(parentname);
			for (FieldDefinition parentField : parentSchema.getFieldDefinitions()) {
				schema.addField(parentField);
			}
		}
		
		EncodedQuery query = new EncodedQuery(dictionary).
			addEquals("name",  tablename).
			addEquals("active", "true").
			addNotNull("element");		
		RestTableReader reader = new RestPetitTableReader(dictionary);
		reader.setFilter(query);
		reader.setFields(FieldDefinition.DICT_FIELDS);
		reader.setPageSize(2000);
		RecordList recs = reader.getAllRecords();
		for (TableRecord rec : recs) {
			String fieldname = rec.getValue("element");
			if (fieldname != null) {
				FieldDefinition fieldDef = new FieldDefinition(table, rec);
				schema.addField(fieldDef);
				logger.debug(Log.BIND, String.format("%s.%s %s(%d)", 
						tablename, fieldname, fieldDef.getType(), fieldDef.getLength()));
			}
		}
						
		if (schema.isEmpty()) {
			logger.error(Log.SCHEMA, "Unable to read schema for: " + tablename +
				" (check access controls for sys_dictionary and sys_db_object)");
			if (tablename.equals("sys_db_object") || tablename.equals("sys_dictionary"))
				throw new InsufficientRightsException("Unable to generate schema for " + tablename);
			else
				throw new InvalidTableNameException(tablename);
		}
		Log.setJobContext(saveJob);
		return schema;
		
	}
	
	private String determineParentName(String tableName) throws IOException {
		Log.setTableContext(hierarchy,  hierarchy.getName() + "." + tableName);
		TableRecord myRec = hierarchy.api().getRecord("name", tableName);
		if (myRec == null) {
			logger.error(Log.SCHEMA, "Unable to read schema for: " + tableName +
					" (check access controls for sys_dictionary and sys_db_object)");
			throw new InvalidTableNameException(tableName);			
		}
		RecordKey parentKey = myRec.getKey("super_class");
		if (parentKey == null) return null;
		TableRecord parentRec = hierarchy.getRecord(parentKey);
		String parentName = parentRec.getValue("name");
		logger.debug(Log.SCHEMA, "parent of " + tableName + " is " + parentKey + "/" + parentName);
		return parentName;
	}
	
	
}
