package sndml.servicenow;

import java.io.PrintStream;
import java.util.Collection;
import java.util.Iterator;
import java.util.TreeMap;

public class TableSchema {

	final protected Table table;
	final protected TreeMap<String,FieldDefinition> fields;
	
	public TableSchema(Table table) {
		this.table = table;
		this.fields = new TreeMap<String,FieldDefinition>();
	}
	
	public boolean isEmpty() {
		return fields.isEmpty();
	}
	
	public void addField(String name, String type, Integer len, String ref) {
		this.addField(new FieldDefinition(table, name, type, len, ref));
	}
	
	public void addField(FieldDefinition field) {
		fields.put(field.getName(), field);
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

	public boolean contains(String fieldname) {
		return fields.containsKey(fieldname);
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
