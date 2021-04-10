package sndml.servicenow;


/**
 * <p>Encapsulates an Encoded Query.</p> 
 * 
 * <p>Most of the methods in the class will return the modified object
 * to support chaining.</p>
 * 
 * <p><b>Warning:</b>
 * This class does NOT check the syntax of the encoded query string.
 * If you construct an encoded query with an invalid field name or
 * an invalid syntax, the behavior of ServiceNow will be to ignore it.</p>
 * 
 */
public class EncodedQuery implements Cloneable {

	// Here are some commonly used encoded query operators
	final public static String EQUALS                 = "=";
	final public static String NOT_EQUAL              = "!=";
	final public static String LESS_THAN              = "<";
	final public static String GREATER_THAN           = ">";
	final public static String LESS_THAN_OR_EQUALS    = "<=";
	final public static String GREATER_THAN_OR_EQUALS = ">=";
	final public static String STARTS_WITH            = "STARTSWITH";
	final public static String CONTAINS               = "LIKE";
	final public static String IN                     = "IN";
	final public static String NOT_IN                 = "NOT IN";
	final public static String IS_EMPTY               = "ISEMPTY";
	final public static String IS_NOT_EMPTY           = "ISNOTEMPTY";
	final public static String ORDER_BY               = "ORDERBY";
	final public static String ORDER_BY_DESC          = "ORDERBYDESC";
	
	private final Table table;
	private final StringBuffer buf;
	
	private static enum OrderBy {NONE, FIELDS, KEYS}; 
	private OrderBy orderBy = OrderBy.NONE;
		
	public EncodedQuery(Table table) {
		this.table = table;
		this.buf = new StringBuffer();
	}
	
	public EncodedQuery(Table table, String str) {
		this.table = table;
		this.buf = str == null ? new StringBuffer() : new StringBuffer(str);
	}

	public EncodedQuery(Table table, KeySet keys) {
		this.table = table;
		this.buf = new StringBuffer();
		this.addQuery(keys);
	}
		
	/**
	 * Make a copy of an EncodedQuery.
	 */
	public EncodedQuery(EncodedQuery other) {
		this.table = other.table;
		this.buf = new StringBuffer(other.buf);
	}
	
	/**
	 * @return A query that will read the entire table, i.e. an empty query
	 */
	public static EncodedQuery all(Table table) {
		return new EncodedQuery(table);
	}
	
	public static boolean isEmpty(EncodedQuery query) {
		if (query == null) return true;
		if (query.isEmpty()) return true;
		return false;
	}
	
	public boolean isEmpty() {
		return (buf.length() == 0);
	}
	
	
	/**
	 * Make a copy of an EncodedQuery
	 */		
	public EncodedQuery clone() {
		return new EncodedQuery(this);
	}

	public String toString() {
		Domain domain = this.table.getDomain();
		if (domain == null) {
			return this.buf.toString();
		}
		else {
			StringBuffer result = new StringBuffer("sys_domainIN" + domain.toString());
			if (buf.length() > 0) {
				result.append('^');
				result.append(this.buf);
			}
			return result.toString();
		}
	}
	
	public boolean equals(Object other) {
		return this.toString().equals(other.toString());
	}
			
	/**
	 * Append an encoded query string to a {@link EncodedQuery}
	 * 
	 * <pre>
	 * filter.{@link #addQuery}("category=network");
	 * </pre>
	 * 
	 * @param str An encoded query string
	 * @return The modified original query.
	 */
	public EncodedQuery addQuery(String str) {
		if (str != null) {
			if (buf.length() > 0) buf.append("^");
			buf.append(str);
		}
		return this;
	}

	/**
	 * Append another {@link EncodedQuery} to this one.
	 * 
	 * @return The modified original query.
	 */
	public EncodedQuery addQuery(EncodedQuery other) {		
		if (other != null) addQuery(other.toString());
		return this;
	}

	/**
	 * Augment a QueryFilter using a name, operator and value.
	 * 
	 * <pre>
	 * QueryFilter filter = new QueryFilter()
	 *    .addFilter("category", "=", "network")
	 *    .addFilter("active", "=", "true");
	 * </pre>
	 * 
	 */
	public EncodedQuery addQuery(String field, String relop, String value) {
		assert field != null;
		assert relop != null;
		if (value == null)
			return addQuery(field + relop);
		else
			return addQuery(field + relop + value);
	}

	/**
	 * Add a list of keys to a query filter.
	 */
	public EncodedQuery addQuery(KeySet keys) {
		assert keys != null;
		return addQuery("sys_id", EncodedQuery.IN, keys.toString());
	}
	
	/**
	 * Add a name/value pair to a QueryFilter.
	 */
	public EncodedQuery addEquals(String field, String value) {
		return addQuery(field, EQUALS, value);
	}
	
	/**
	 * Add not null query
	 */
	public EncodedQuery addNotNull(String field) {
		return addQuery(field + IS_NOT_EMPTY);
	}
	
	/**
	 * Adds a datetime range to a filter for sys_updated_on.
	 * 
	 * @param starting Select records updated on or after this datetime
	 * @param ending Select records updated before this datetime
	 * @return The modified original filter
	 */
	public EncodedQuery addUpdated(DateTime starting, DateTime ending) {
		if (starting != null) this.addQuery("sys_updated_on>=" + starting.toString());
		if (ending   != null) this.addQuery("sys_updated_on<" + ending.toString());
		return this;
	}

	public EncodedQuery addUpdated(DateTimeRange updated) {
		addUpdated(updated.getStart(), updated.getEnd());
		return this;
	}
	
	/**
	 * Adds a datetime range to a filter for sys_created_on.
	 * 
	 * @param starting Select records created on or after this datetime
	 * @param ending Select records created before this datetime
	 * @return The modified original filter
	 */
	public EncodedQuery addCreated(DateTime starting, DateTime ending) {
		if (starting != null) this.addQuery("sys_created_on>=" + starting.toString());
		if (ending   != null) this.addQuery("sys_created_on<" + ending.toString());
		return this;
	}
	
	public EncodedQuery addCreated(DateTimeRange created) {
		return addCreated(created.getStart(), created.getEnd());
	}
	
	public EncodedQuery addOrderByKeys() {
		assert this.orderBy != OrderBy.FIELDS;
		this.addQuery(ORDER_BY + "sys_id");
		this.orderBy = OrderBy.KEYS;
		return this;
	}
	
	/**
	 * Exclude all sys_ids less than or equal to the specified value.
	 * Only applicable if orderByKeys was previously called.
	 * 
	 * @param value max sys_id from previous query
	 * @return The modified original filter
	 */
	public EncodedQuery excludeKeys(RecordKey value) {
		if (value != null) {
			this.addQuery("sys_id", GREATER_THAN, value.toString());			
		}
		return this;
	}	

	public EncodedQuery addOrderBy(String fieldname) {
		assert this.orderBy != OrderBy.KEYS;		
		this.addQuery(ORDER_BY + fieldname);
		this.orderBy = OrderBy.FIELDS;
		return this;
	}
	
	public EncodedQuery addOrderByDesc(String fieldname) {
		assert this.orderBy != OrderBy.KEYS;
		this.addQuery(ORDER_BY_DESC + fieldname);
		this.orderBy = OrderBy.FIELDS;
		return this;
	}
	
}
