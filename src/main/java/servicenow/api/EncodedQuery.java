package servicenow.api;


/**
 * Encapsulates an Encoded Query.  
 * <p/>
 * {@link EncodedQuery} is used to restrict the number
 * of records return by a {@link TableReader}.  
 * The following two examples are equivalent.
 * <p/>
 * <b>Example 1:</b>
 * <pre>
 * {@link EncodedQuery} filter = new {@link EncodedQuery}("category=network^active=true");
 * </pre>
 * <p/>
 * <b>Example 2:</b>
 * <pre>
 * {@link EncodedQuery} filter = new {@link EncodedQuery}()
 *    .{@link #addQuery}("category", "network")
 *    .{@link #addQuery}("active", "true");
 * </pre>
 * 
 * Most of the methods in the class will return the modified object
 * so that it is easy to chain calls together as in Example 2 above.
 * <p/>
 * For an explanation of encoded query syntax refer to
 * <a href="http://wiki.servicenow.com/index.php?title=Reference_Qualifiers"
 * >http://wiki.servicenow.com/index.php?title=Reference_Qualifiers</a>
 * <p/>
 * <b>Warning:</b>
 * This class does NOT check the syntax of the encoded query string.
 * If you construct an encoded query with an invalid field name or
 * an invalid syntax, the behavior of ServiceNow will be to ignore it.
 * 
 * @author Giles Lewis
 */
public class EncodedQuery {

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
	final public static String ORDER_BY               = "ORDERBY";
	final public static String ORDER_BY_DESC          = "ORDERBYDESC";
	
	private StringBuffer buf = new StringBuffer();

	/**
	 * Create an empty EncodedQuery.
	 */
	public EncodedQuery() {
	}

	/**
	 * @return A query that will read the entire table, i.e. an empty query
	 */
	public static EncodedQuery all() {
		return new EncodedQuery();
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
	 * Create an EncodedQuery from a string.
	 * 
	 * <pre>
	 * {@link EncodedQuery} filter = new {@link EncodedQuery}("category=network^active=true");
	 * </pre>
	 */
	public EncodedQuery(String str) {
		if (str != null) buf.append(str);
	}

	/**
	 * Make a copy of an EncodedQuery.
	 */
	public EncodedQuery(EncodedQuery other) {
		if (other != null) addQuery(other.toString());
	}

	/**
	 * Make a copy of an EncodedQuery
	 */	
	public EncodedQuery copy() {
		return new EncodedQuery(this);
	}
	
	public EncodedQuery(String field, String relop, String value) {
		this.addQuery(field, relop, value);
	}
	
	public EncodedQuery(String field, String value) {
		this.addQuery(field, EQUALS, value);
	}
	
	public EncodedQuery(KeySet keys) {
		this.addQuery(keys);
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

	public EncodedQuery addOrderBy(String fieldname) {
		this.addQuery(ORDER_BY + fieldname);
		return this;
	}
	
	public EncodedQuery addOrderByDesc(String fieldname) {
		this.addQuery(ORDER_BY_DESC + fieldname);
		return this;
	}
	
	public String toString() {
		return buf.toString();
	}
	
}
