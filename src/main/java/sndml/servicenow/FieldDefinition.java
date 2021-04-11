package sndml.servicenow;

/**
 * Contains data type information for a single field in a ServiceNow table.
 * @author Giles Lewis
 *
 */
public class FieldDefinition {

	private final Table table;
	private final String name;
	private final String type;
	private final Integer max_length;
	private final String ref_table;
	public static final FieldNames DICT_FIELDS = new FieldNames("element,internal_type,max_length,reference");
	
	/**
	 * Construct a FieldDefinition from sys_dictionary record.
	 * 
	 * @param table - The table in which this field appears.
	 * @param dictrec - The sys_dictionary record that describes this field.
	 */
	protected FieldDefinition(Table table, TableRecord dictrec) {
		this(table, dictrec.getValue("element"), dictrec.getValue("internal_type"), 
				dictrec.getInteger("max_length"), dictrec.getValue("reference"));
	}

	public FieldDefinition(Table table, String name, String type, Integer len, String ref) {
		if (name == null)
			throw new AssertionError(String.format(
				"Missing name for field in \"%s\". Check sys_dictionary read permissions.", 
				table.getName()));
		if (type == null) 
			throw new AssertionError(String.format(
				"Field \"%s.%s\" has no type. Check sys_dictionary read permissions.", 
				table.getName(), name));  
		this.table = table;
		this.name = name;
		this.type = type;
		this.max_length = len;
		this.ref_table = ref;		
	}
	
	/**
	 * Return the table
	 */
	public Table getTable() {
		return table;
	}
	
	/**
	 * Return the name of this field.
	 */
	public String getName() { 
		return name; 
	}
	
	/**
	 * Return the type of this field.
	 */
	public String getType() { 
		return type; 
	}
	
	/**
	 * Return the length of this field.
	 */
	public int getLength() { 
		return max_length; 
	}
	
	/**
	 * If this is a reference field then return the name of the
	 * referenced table.  Otherwise return null.
	 */
	public String getReference() { 
		return ref_table; 
	}
	
	/**
	 * Return true if the field is a reference field.
	 * The value of a reference field is always a {@link RecordKey} (sys_id).
	 */
	public boolean isReference() { 
		return (ref_table != null && ref_table.length() > 0); 
	}
		
}
