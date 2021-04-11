package sndml.servicenow;

import java.io.*;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class holds the the schema or definition for a ServiceNow table.
 * The definition is read from <b>sys_dictionary</b> by the {@link Table} constructor.
 * 
 */
@Deprecated
public class LegacyTableSchema {

	private final Table table;
	private final Session session;
	private final String tablename;
	private final String parentname;
	private final Table dictionary;
	private final Table hierarchy;
	
	private boolean empty = true;
	private TreeMap<String,FieldDefinition> fields;
	final private Logger logger = LoggerFactory.getLogger(this.getClass());
	
	protected LegacyTableSchema(Table table) 
			throws IOException, InvalidTableNameException, InterruptedException {
		this.table = table;
		this.session = table.session;
		this.tablename = table.getName();
		dictionary = session.table("sys_dictionary");
		hierarchy = session.table("sys_db_object");
		String saveJob = Log.getJobContext();
		String myname = dictionary.getName() + "." + this.tablename;
		Log.setTableContext(dictionary,  myname);
		logger.debug(Log.SCHEMA, "get definition for table " + tablename);
		fields = new TreeMap<String,FieldDefinition>();
		parentname = determineParentName();
		logger.debug(Log.SCHEMA, tablename + " parent is " + parentname);
		if (parentname != null) {
			// recursive call for parent definition
			Table parentTable = session.table(parentname);
			LegacyTableSchema parentDefinition = new LegacyTableSchema(parentTable);
			Iterator<FieldDefinition> parentIter = parentDefinition.iterator();
			while (parentIter.hasNext()) {
				FieldDefinition parentField = parentIter.next();
				String fieldname = parentField.getName();
				fields.put(fieldname, parentField);
			}
		}
		
		EncodedQuery query = new EncodedQuery(dictionary).
			addEquals("name",  tablename).
			addEquals("active", "true");
		
		RestTableReader reader = new RestPetitTableReader(dictionary);
		reader.setFilter(query);
		reader.setFields(FieldDefinition.DICT_FIELDS);
		reader.setPageSize(5000);
		RecordList recs = reader.getAllRecords();
		processRecords(recs);
				
		if (this.empty) {
			logger.error(Log.SCHEMA, "Unable to read schema for: " + tablename +
				" (check access controls for sys_dictionary and sys_db_object)");
			if (tablename.equals("sys_db_object") || tablename.equals("sys_dictionary"))
				throw new InsufficientRightsException("Unable to generate schema for " + tablename);
			else
				throw new InvalidTableNameException(tablename);
		}
		Log.setJobContext(saveJob);
	}

	public void processRecords(RecordList recs) throws IOException {
		for (TableRecord rec : recs) {
			String fieldname = rec.getValue("element");
			if (fieldname != null) {
				FieldDefinition fieldDef = new FieldDefinition(table, rec);
				fields.put(fieldname, fieldDef);
				logger.debug(Log.BIND, String.format("%s.%s %s(%d)", 
						tablename, fieldname, fieldDef.getType(), fieldDef.getLength()));
				this.empty = false;
			}
		}
	}
	
	private String determineParentName() throws IOException {
		// if (tablename.startsWith("sys_")) return null;
		Log.setTableContext(hierarchy,  hierarchy.getName() + "." + this.tablename);
		TableRecord myRec = hierarchy.api().getRecord("name", this.tablename);
		if (myRec == null) {
			logger.error(Log.SCHEMA, "Unable to read schema for: " + tablename +
					" (check access controls for sys_dictionary and sys_db_object)");
			throw new InvalidTableNameException(tablename);			
		}
		RecordKey parentKey = myRec.getKey("super_class");
		if (parentKey == null) return null;
		TableRecord parentRec = hierarchy.getRecord(parentKey);
		String parentName = parentRec.getValue("name");
		logger.debug(Log.SCHEMA, "parent of " + tablename + " is " + parentKey + "/" + parentName);
		return parentName;
	}
	
	/**
	 * Return the name of the table from which this table was extended.
	 * 
	 * @return The name of the parent table or null if this table has no parent.
	 */
	public String getParentName() {
		return parentname;
	}
	
	/**
	 * Return a list of all the fields in a table.
	 */
	public FieldNames getFieldNames() {
		return new FieldNames(fields.keySet());
	}
	
	/**
	 * Return all fields minus a set of names
	 */
	public FieldNames getFieldsMinus(FieldNames minus) {
		FieldNames result = new FieldNames();
		for (String name : getFieldNames()) {
			if (!minus.contains(name)) result.add(name);
		}
		return result;
	}

	/**
	 * Return the type definition for a field.
	 */
	public FieldDefinition getFieldDefinition(String fieldname) {
		return fields.get(fieldname);
	}

	/**
	 * Return the type definition for the sys_id.
	 */
	public FieldDefinition getKeyFieldDefinition() {
		return fields.get("sys_id");
	}
	
	/**
	 * Return true if the table has a reference field with this name; otherwise false.
	 */
	public boolean isReference(String fieldname) {
		FieldDefinition fd = fields.get(fieldname);
		if (fd == null) return false;
		return fd.isReference();
	}

	/**
	 * Return a collection of all the field type definitions.
	 */
	public Collection<FieldDefinition> getFieldDefinitions() {
		return fields.values();
	}
	
	/**
	 * Return an iterator which loops through all the field definitions.
	 */
	public Iterator<FieldDefinition> iterator() {
		return getFieldDefinitions().iterator();
	}

	/**
	 * Return the number of fields in this table.
	 */
	public int numFields() {
		return fields.size();
	}

	/**
	 * Used for testing
	 */
	public void report(PrintStream out) {
		String tablename = table.getName();
		out.println("Schema report for " + tablename);
		int row = 0;
		Iterator<FieldDefinition> iter = iterator();
		while (iter.hasNext()) {
			FieldDefinition fd = iter.next();
			String fieldname = fd.getName();
			String fieldtype = fd.getType();
			int fieldlen = fd.getLength();
			out.println(++row + " " + fieldname + " " + fieldtype + " " + fieldlen);			
		}
		out.println("End");
	}

}
