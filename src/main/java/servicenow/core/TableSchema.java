package servicenow.core;

import java.io.*;
import java.sql.SQLException;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class holds the the schema or definition for a ServiceNow table.
 * The definition is read from <b>sys_dictionary</b> by the {@link Table} constructor.
 * 
 * @author Giles Lewis
 *
 */
public class TableSchema extends Writer {

	private final Table table;
	private final Session session;
	private final String tablename;
	private final String parentname;
	private final Table dictionary;
	private final Table hierarchy;
	
	private boolean empty = true;
	private TreeMap<String,FieldDefinition> fields;
	final private Logger logger = LoggerFactory.getLogger(this.getClass());
	
	protected TableSchema(Table table) 
			throws IOException, 	InvalidTableNameException, InterruptedException {
		super();
		this.table = table;
		this.session = table.session;
		this.tablename = table.getName();
		logger.debug(Log.INIT, "get definition for table " + tablename);
		fields = new TreeMap<String,FieldDefinition>();
		dictionary = session.table("sys_dictionary");
		hierarchy = session.table("sys_db_object");
		parentname = determineParentName();
		logger.debug(Log.INIT, tablename + " parent is " + parentname);
		if (parentname != null) {
			// recursive call for parent definition
			Table parentTable = session.table(parentname);
			// TODO: optimize this code with a cache of schemas at the Session level
			TableSchema parentDefinition = new TableSchema(parentTable);
			Iterator<FieldDefinition> parentIter = parentDefinition.iterator();
			while (parentIter.hasNext()) {
				FieldDefinition parentField = parentIter.next();
				String fieldname = parentField.getName();
				fields.put(fieldname, parentField);
			}
		}
		
		EncodedQuery query = new EncodedQuery().
			addEquals("name",  tablename).
			addEquals("active", "true");
		
		TableReader reader = dictionary.getDefaultReader();
		Log.setContext(dictionary,  dictionary.getName() + "." + this.tablename);
		reader.setWriter(this);
		reader.setBaseQuery(query);
		reader.setFields(FieldDefinition.DICT_FIELDS);
		reader.setPageSize(1000);
		reader.initialize();
		try {
			reader.call();
		} catch (SQLException e) {
			throw new ServiceNowError(e);
		}
		
		if (this.empty) {
			logger.error(Log.INIT, "Unable to read schema for: " + tablename +
				" (check access controls for sys_dictionary and sys_db_object)");
			if (tablename.equals("sys_db_object") || tablename.equals("sys_dictionary"))
				throw new InsufficientRightsException(dictionary.getName(), 
						"Unable to generate schema for " + tablename);
			else
				throw new InvalidTableNameException(tablename);
		}
	}

	@Override
	public void processRecords(RecordList recs) throws IOException, SQLException {
		for (Record rec : recs) {
			String fieldname = rec.getValue("element");
			if (fieldname != null) {
				FieldDefinition fieldDef = new FieldDefinition(table, rec);
				fields.put(fieldname, fieldDef);
				writerMetrics.incrementInserted();
				logger.debug(String.format("%s.%s %s(%d)", 
						tablename, fieldname, fieldDef.getType(), fieldDef.getLength()));
				this.empty = false;
			}
		}
	}
	
	private String determineParentName() throws IOException {
		if (tablename.startsWith("sys_")) return null;
		Log.setContext(hierarchy,  hierarchy.getName() + "." + this.tablename);
		Record myRec = hierarchy.getRecord("name", this.tablename, false);
		if (myRec == null) {
			logger.error(Log.INIT, "Unable to read schema for: " + tablename +
					" (check access controls for sys_dictionary and sys_db_object)");
			throw new InvalidTableNameException(tablename);			
		}
		Key parentKey = myRec.getKey("super_class");
		if (parentKey == null) return null;
		Record parentRec = hierarchy.getRecord(parentKey);
		String parentName = parentRec.getValue("name");
		logger.debug("parent of " + tablename + " is " + parentKey + "/" + parentName);
		return parentName;
	}
	
	/**
	 * Return the name of the parent table or null if this table has no parent.
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
