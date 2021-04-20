package sndml.datamart;

import java.io.*;
import java.net.URI;
import java.util.*;

import org.apache.commons.cli.*;
import org.jdom2.*;
import org.jdom2.input.SAXBuilder;
import org.slf4j.Logger;

import sndml.servicenow.*;

/**
 * A class which knows how to generate SQL statements (as strings)
 * based on the ServiceNow schema and information in the <tt>templates.xml</tt> file.
 *
 */
public class Generator {

	enum NameCase {UPPER, LOWER, AUTO};
	enum NameQuotes {DOUBLE, SQUARE, NONE};
	
	private final Document xmldocument;
	private final Element dialectTree;
	private final boolean autocommit;
	private final NameCase namecase; 
	private final NameQuotes namequotes;
	private final String schemaName;
	private final NameMap namemap;
	
	Logger logger = Log.logger(this.getClass());
	
	@SuppressWarnings("serial")
	class Variables extends HashMap<String,String> {		
	}
	
	/**
	 * Generate a Create Table statement.
	 * 
	 * @param args -p <i>profile</i> -t <i>tablename</i>
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		Options options = new Options();
		options.addOption(Option.builder("p").longOpt("profile").required(true).hasArg().
				desc("Profile file name (required)").build());
		options.addOption(Option.builder("t").longOpt("table").required(true).hasArg().
				desc("Table name (required)").build());
		options.addOption(Option.builder("o").longOpt("output").required(false).hasArg().
				desc("Table name (required)").build());		
		CommandLine cmdline = new DefaultParser().parse(options, args);
		String profilename = cmdline.getOptionValue("p");
		String tablename = cmdline.getOptionValue("t");
		PrintStream output = cmdline.hasOption("o") ? 
			new PrintStream(new File(cmdline.getOptionValue("o"))) : System.out;
		ConnectionProfile profile = new ConnectionProfile(new File(profilename));
		Session session = profile.getSession();
		Table table = session.table(tablename);
		Database database = profile.getDatabase();
		Generator generator = new Generator(database, profile, null);
		String sql = generator.getCreateTable(table);
		output.print(sql);		
	}
	
	public Generator(Database database, ConnectionProfile profile, File templatesFile) {
		assert database != null;
		String schemaName = profile.getProperty("datamart.schema");
		String dialectName = profile.getProperty("datamart.dialect");
		// only check properties if file was not passed in as an argument
		if (templatesFile == null) {
			// TODO Redundant. Same code appears in Database.
			String templatesPath = profile.getProperty("datamart.templates", "");
			if (templatesPath.length() > 0)	templatesFile = new File(templatesPath);			
		}
		try {
			// if file not specified as argument or property
			// then use the default XML from the JAR
			InputStream sqlConfigStream =
				(templatesFile == null) ?
				ClassLoader.getSystemResourceAsStream("sqltemplates.xml") :
				new FileInputStream(templatesFile);	
			SAXBuilder xmlbuilder = new SAXBuilder();
			xmldocument = xmlbuilder.build(sqlConfigStream);
		} 
		catch (IOException | JDOMException e2) {
			throw new ResourceException(e2);
		}
		
		if (dialectName == null || dialectName.length() == 0) 
			this.dialectTree = getProtocolTree(database.getURI());
		else
			this.dialectTree = getDialectTree(dialectName);
		
		this.schemaName = schemaName;
		this.namemap = new NameMap(dialectTree.getChild("fieldnames"));
		
		Element dialogProps = dialectTree.getChild("properties");
				
		autocommit = Boolean.parseBoolean(dialogProps.getChildText("autocommit").toLowerCase());
		namecase = NameCase.valueOf(dialogProps.getChildText("namecase").toUpperCase());
		namequotes = NameQuotes.valueOf(dialogProps.getChildText("namequotes").toUpperCase());
		
		logger.info(Log.INIT, String.format(
				"dialect=%s schema=%s namecase=%s namequotes=%s autocommit=%b", 
				getDialectName(), getSchemaName(), namecase.toString(), 
				namequotes.toString(), getAutoCommit()));
	}
	
	Element getProtocolTree(URI dbURI) {
		String protocol = Database.getProtocol(dbURI);
		ListIterator<Element> children = xmldocument.getRootElement().getChildren().listIterator();
		while (children.hasNext()) {
			Element tree = children.next();
			if (tree.getName().equals("sql")) {
				Element drivers = tree.getChild("drivers");
				ListIterator<Element> iter = drivers.getChildren("driver").listIterator();
				while (iter.hasNext()) {
					String driver = iter.next().getTextTrim();
					if (protocol.equals(driver)) return tree;
				}
			}
		}
		return getDialectTree("default");
	}
	
	Element getDialectTree(String dialectName) {
		ListIterator<Element> children = xmldocument.getRootElement().getChildren().listIterator();
		while (children.hasNext()) {
			Element tree = children.next();
			if (tree.getName().equals("sql")) {
				if (tree.getAttributeValue("dialect").equals(dialectName))
					return tree;
			}
		}
		throw new ResourceException("No sql found for dialect='" + dialectName + "'");
	}
	
	static String getDialectName(Element tree) {
		return tree.getAttributeValue("dialect");
	}
	
	String getDialectName() { 
		return getDialectName(dialectTree);
	}
	
	boolean getAutoCommit() {
		return this.autocommit;
	}
	
	List<String> getInitializations() {
		Variables myvars = new Variables();
		myvars.put("schema", this.schemaName);
		List<String> result = new ArrayList<String>();
		Element initialize = dialectTree.getChild("initialize");
		ListIterator<Element> iter = initialize.getChildren("statement").listIterator();
		while (iter.hasNext()) {
			String stmt = iter.next().getTextTrim();
			stmt = replaceVars(stmt, myvars);
			result.add(stmt);
		}
		return result;
	}
		
	String sqlCase(String name) {
		if (namecase.equals(NameCase.LOWER))
			return name.toLowerCase();
		else
			return name.toUpperCase();
	}
	
	String sqlQuote(String name) {
		if (namequotes.equals(NameQuotes.DOUBLE)) 
			return "\"" + name + "\"";
		else if (namequotes.equals(NameQuotes.SQUARE)) 
			return "[" + name + "]";
		else
			return name;
	}
		
	String sqlName(String name) {
		String lookup = namemap.getSqlName(name);
		return (lookup == null) ? sqlQuote(sqlCase(name)) : lookup;
	}
	
	String getSchemaName() {
		return schemaName;
	}
	
	/**
	 * Return a qualfied table name as schema.table
	 */
	String sqlTableName(String name) {
		String result = sqlCase(name);
		if (schemaName != null && schemaName.length() > 0) 
			result = schemaName + "." + result;
		return result;
	}
	
	String glideName(String name) {
		String result = namemap.getGlideName(name);
		return (result == null) ? name.toLowerCase() : result;
	}

	/**
	 * Return the SQL Type corresponding to a Glide Type
	 */
	private String sqlType(String glidetype, int size) {
		String sqltype = null;
		ListIterator<Element> alltypes = dialectTree.getChild("datatypes").
			getChildren("typemap").listIterator();
		while (sqltype == null && alltypes.hasNext()) {
			Element ele = alltypes.next();
			String attr = ele.getAttributeValue("glidetype");
			if (attr.equals(glidetype) || attr.equals("*")) {
				String minsize = ele.getAttributeValue("minsize");
				String maxsize = ele.getAttributeValue("maxsize");
				if (minsize != null && size < Integer.parseInt(minsize)) continue;
				if (maxsize != null && size > Integer.parseInt(maxsize)) continue;
				sqltype = ele.getTextTrim();
				if (sqltype.indexOf("#") > -1)
					sqltype = sqltype.replaceAll("#", Integer.toString(size));
			}
		}
		return sqltype;
	}
	
	String getTemplate(String templateName, Table table, Map<String,String> vars) {
		return getTemplate(templateName, table.getName(), vars);
	}
	
	String getTemplate(String templateName, String tableName) {
		return getTemplate(templateName, tableName, null);
	}
	
	String getTemplate(
			String templateName, 
			String tableName,
			Map<String,String> vars) {
		String sql = dialectTree.getChild("templates").getChildText(templateName);
		assert sql != null : "Template not found: " + templateName;
		Variables myvars = new Variables();
		myvars.put("schema", this.schemaName);
		myvars.put("table", sqlCase(tableName));
		myvars.put("keyvalue", "?");
		if (vars != null) myvars.putAll(vars);
		return replaceVars(sql, myvars);
	}
	
	String getCreateTable(Table table) throws IOException, InterruptedException {
		return getCreateTable(table, table.getName(), null);
	}
	
	String getCreateTable(Table table, String sqlTableName) throws IOException, InterruptedException {
		return getCreateTable(table, sqlTableName, null);
	}
	
	String getCreateTable(Table table, String sqlTableName, FieldNames includeColumns) 
			throws IOException, InterruptedException {
		assert sqlTableName != null;
		// We may be pulling the schema from a different ServiceNow instance
		TableSchema tableSchema = table.getSchema();
		TableWSDL tableWSDL = table.getWSDL();
		final String fieldSeparator = ",\n";
		StringBuilder fieldlist = new StringBuilder();
		// Make sys_id the first field.
		// All tables have sys_id.
		fieldlist.append(sqlFieldDefinition(tableSchema.getFieldDefinition("sys_id")));
		Iterator<FieldDefinition> iter = tableSchema.iterator();
		while (iter.hasNext()) {
			FieldDefinition fd = iter.next();
			String name = fd.getName();
			if (name.equals("sys_id")) {
				// sys_id already added above
				continue;
			}
			if (includeColumns != null) {
				if (!includeColumns.contains(name)) continue;
			}
			if (!tableWSDL.canReadField(name)) {
				// it is in the dictionary, but not the WSDL
				// it could be blocked by an access control
				// skip it
				continue;
			}
			fieldlist.append(fieldSeparator);
			fieldlist.append(sqlFieldDefinition(fd));
		}
		Variables map = new Variables();
		map.put("fielddefinitions", fieldlist.toString());
		String result = getTemplate("create", sqlTableName, map);
		return result;
	}
	
	private String sqlFieldDefinition(FieldDefinition fd) {
		String fieldname = fd.getName();
		String fieldtype = fd.getType();
		assert fieldname != null;
		assert fieldtype != null;
		int size = fd.getLength();
		String sqlname = sqlName(fieldname);
		String sqltype = sqlType(fieldtype, size);
		logger.trace(Log.INIT, fieldname + " " + fieldtype + "->" + sqltype);
		if (sqltype == null) {
			String descr = sqlname + " " + fieldtype + "(" + size + ")";
			logger.warn(Log.INIT, "Field type not mapped " + descr);
			return "-- " + descr;
		}
		String result = sqlname + " " + sqltype;
		return result;
	}

	@Deprecated
	static String replaceVars(String sql, Properties vars) {
		assert sql != null;
		for (String name : vars.stringPropertyNames()) {
			String value = vars.getProperty(name);
			if (value == null || value.length() == 0) {
				// if the variable (e.g. $schema) is null or zero length
				// then also consume a period following the variable
				// i.e. "insert into $schema.$table" becomes "insert into $table"
				sql = sql.replaceAll("\\$\\{" + name + "\\}\\.?", "");
				sql = sql.replaceAll("\\$" + name + "\\.", "");
				sql = sql.replaceAll("\\$" + name + "\\b", "");
			} else {
				sql = sql.replaceAll("\\$\\{" + name + "\\}", value);
				sql = sql.replaceAll("\\$" + name + "\\b", value);
			}			
		}
		return sql;
	}
	
	static String replaceVars(String sql, Map<String,String> vars) {
		assert sql != null;
		Iterator<Map.Entry<String, String>> iterator = vars.entrySet().iterator();
		while (iterator.hasNext()) {
			Map.Entry<String, String> entry = iterator.next();
			String name = entry.getKey();
			String value = entry.getValue();
			assert name != null;
			if (value == null || value.length() == 0) {
				// if the variable (e.g. $schema) is null or zero length
				// then also consume a period following the variable
				// i.e. "insert into $schema.$table" becomes "insert into $table"
				sql = sql.replaceAll("\\$\\{" + name + "\\}\\.?", "");
				sql = sql.replaceAll("\\$" + name + "\\.", "");
				sql = sql.replaceAll("\\$" + name + "\\b", "");
			} else {
				sql = sql.replaceAll("\\$\\{" + name + "\\}", value);
				sql = sql.replaceAll("\\$" + name + "\\b", value);
			}
		}
		return sql;
	}
}
