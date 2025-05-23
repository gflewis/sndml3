package sndml.loader;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.*;

import org.apache.commons.cli.*;
import org.jdom2.*;
import org.jdom2.input.SAXBuilder;
import org.slf4j.Logger;

import sndml.servicenow.*;
import sndml.util.FieldNames;
import sndml.util.Log;
import sndml.util.ResourceException;

/**
 * A class which knows how to generate SQL statements (as strings)
 * based on the ServiceNow schema and information in the <code>templates.xml</code> file.
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
	private final SchemaReader schemaReader;
	
	Logger logger = Log.getLogger(this.getClass());
	
	@SuppressWarnings("serial")
	class Variables extends HashMap<String,String> {		
	}
	
	/**
	 * Generate a Create Table statement.
	 * 
	 * @param args -p <i>profile</i> -t <i>tablename</i>
	 * @throws Exception
	 */
	@SuppressWarnings("deprecation")
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
		Session session = new Session(profile.reader);
		Table table = session.table(tablename);
		DatabaseWrapper database = new DatabaseWrapper(profile);
		Generator generator = database.getGenerator();
		String sql = generator.getCreateTable(table);
		output.print(sql);		
	}
	
	public Generator(InputStream templatesStream, Properties properties, SchemaReader schemaReader) 
				throws ResourceException {
		
		String dialectName = properties.getProperty("dialect", null);
		String schemaName = properties.getProperty("schema", "");
		
		try {
			SAXBuilder xmlbuilder = new SAXBuilder();
			xmldocument = xmlbuilder.build(templatesStream);			
		}
		catch (IOException | JDOMException e) {
			throw new ResourceException(e);
		}
		
		// TODO: use DatabaseWrapper.protocolFromProfile
		if (dialectName == null) {
			String dburl = properties.getProperty("url", null);
			// Infer dialect from the URL
			URI dburi;
			try {
				dburi = new URI(dburl);
			} catch (URISyntaxException e) {
				throw new ResourceException(e);
			}
			assert dburi != null;
			this.dialectTree = getProtocolTree(dburi);				
		}
		else {
			// Dialect was explicitly specified as a property
			this.dialectTree = getDialectTree(dialectName);
		}
		
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
		
		this.schemaReader = schemaReader;
		assert this.schemaReader != null;
	}
	
	@Deprecated
	public Generator(ConnectionProfile profile) throws ResourceException {
		this(getTemplatesStream(profile.database), 
				profile.databaseProperties(), 
				profile.newSchemaReader());
				
	}
	
	/**
	 * Get an InputStream based on the name of the "templates" property.
	 * @param properties
	 * @return
	 */	
	public static InputStream getTemplatesStream(Properties properties) {
		String templatesPath = properties.getProperty("templates", "");
		File templatesFile = null;
		if (templatesPath.length() > 0)	templatesFile = new File(templatesPath);			
		
		// TODO This code is problematic in a web app
		try {
			// if file not specified as property
			// then use the default XML from the JAR
			InputStream inputStream =
				(templatesFile == null) ?
				ClassLoader.getSystemResourceAsStream("sqltemplates.xml") :
				new FileInputStream(templatesFile);
			return inputStream;
		} 
		catch (IOException e) {
			throw new ResourceException(e);
		}		
	}
	
	private Element getProtocolTree(URI dbURI) {		
		String protocol = getProtocol(dbURI);
		assert protocol != null;
		ListIterator<Element> children = xmldocument.getRootElement().getChildren().listIterator();
		assert children != null;
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
	
	static String getProtocol(URI dbURI) {
		String urlPart[] = dbURI.toString().split(":");
		return urlPart[1];		
	}
		
	private Element getDialectTree(String dialectName) {
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
	
	/**
	 * Initialize a Connection based on the "initialize" statements
	 * in the template.
	 */
	public void initialize(java.sql.Connection dbc) throws SQLException {
		dbc.setAutoCommit(this.autocommit);
		java.sql.Statement stmt = dbc.createStatement();
		Iterator<String> iter = this.getInitializations().listIterator();
		while (iter.hasNext()) {
			String sql = iter.next();
			logger.info(Log.INIT, sql);
			stmt.execute(sql);
		}
		stmt.close();
		if (!this.autocommit) dbc.commit();
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
		TableSchema tableSchema = schemaReader.getSchema(table.getName());
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
