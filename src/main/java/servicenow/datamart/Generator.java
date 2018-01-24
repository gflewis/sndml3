package servicenow.datamart;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;
import org.slf4j.Logger;

import servicenow.core.*;
import servicenow.soap.TableWSDL;

public class Generator {

	enum NameCase {UPPER, LOWER, AUTO};
	enum NameQuotes {DOUBLE, SQUARE, NONE};
	
	private final Element dialectTree;
	private final NameCase namecase; 
	private final NameQuotes namequotes;
	private final String schemaName;
	private final NameMap namemap;;
	
	Logger logger = Log.logger(this.getClass());

	static Document xmldocument = null;
	
	public static void main(String[] args) throws Exception {
		Globals.initialize(args);
		Session session = Globals.getSession();
		String tablename = Globals.getArg(0);
		Table table = session.table(tablename);
		Generator generator = new Generator("mysql", null);
		System.out.println(generator.getCreateTable(table, tablename));
	}

	public Generator(URI dbURI, String schemaName) {
		this(getTree(dbURI), schemaName);
	}
	
	public Generator(String dialectName, String schemaName) {
		this(getTree(dialectName), schemaName);
	}
	
	public Generator(Element dialectTree, String schemaName) {
		assert dialectTree != null;
		this.dialectTree = dialectTree;
		this.schemaName = schemaName;

		this.namemap = new NameMap(dialectTree.getChild("fieldnames"));
		Element dialogProps = dialectTree.getChild("properties");
				
		namecase = NameCase.valueOf(dialogProps.getChildText("namecase").toUpperCase());
		namequotes = NameQuotes.valueOf(dialogProps.getChildText("namequotes").toUpperCase());
		
		logger.info(Log.INIT, String.format("dialect=%s schema=%s namecase=%s namequotes=%s", 
				getDialectName(), getSchemaName(), namecase.toString(), namequotes.toString()));
		
	}

	static private Document getDocument() {
		if (xmldocument == null) {
			try {
				InputStream sqlConfigStream;
				String path = Globals.getValue("templates");
				if (path == null) 
					sqlConfigStream = ClassLoader.getSystemResourceAsStream("sqltemplates.xml");
				else
					sqlConfigStream = new FileInputStream(new File(path));
				SAXBuilder xmlbuilder = new SAXBuilder();
				xmldocument = xmlbuilder.build(sqlConfigStream);
			} 
			catch (IOException | JDOMException e2) {
				throw new ResourceException(e2);
			}
		}
		assert xmldocument != null;
		return xmldocument;	
	}
	
	static Element getTree(URI dbURI) {
		String dialectName = Globals.getValue("dialect");
		if (dialectName != null) return getTree(dialectName);
		String protocol = Database.getProtocol(dbURI);
		ListIterator<Element> children = getDocument().getRootElement().getChildren().listIterator();
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
		return getTree("default");
	}
	
	static Element getTree(String dialectName) {
		ListIterator<Element> children = getDocument().getRootElement().getChildren().listIterator();
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
	
	List<String> getInitializations() {
		Map<String,String> myvars = new HashMap<String,String>();
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
	String sqlType(String glidetype, int size) {
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
	
	String getTemplate(String templateName, String tableName) {
		return getTemplate(templateName, tableName, null);
	}
	
	String getTemplate(
			String templateName, 
			String tableName,
			Map<String,String> vars) {
		String sql = dialectTree.getChild("templates").getChildText(templateName);
		Map<String,String> myvars = new HashMap<String,String>();
		myvars.put("schema", this.schemaName);
		myvars.put("table", sqlCase(tableName));
		myvars.put("keyvalue", "?");
		if (vars != null) myvars.putAll(vars);
		return replaceVars(sql, myvars);
	}
	
	String getCreateTable(Table table, String sqlTableName) throws IOException, InterruptedException {
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
			if (fd.getName().equals("sys_id")) {
				// sys_id already added above
				continue;
			}
			if (!tableWSDL.canReadField(fd.getName())) {
				// it is in the dictionary, but not the WSDL
				// it could be blocked by an access control
				// skip it
				continue;
			}
			fieldlist.append(fieldSeparator);
			fieldlist.append(sqlFieldDefinition(fd));
		}
		HashMap<String, String> map = new HashMap<String, String>();
		map.put("fielddefinitions", fieldlist.toString());
		String result = getTemplate("create", sqlTableName, map);
		return result;
	}

	String sqlFieldDefinition(FieldDefinition fd) {
		String fieldname = fd.getName();
		String fieldtype = fd.getType();
		assert fieldname != null;
		assert fieldtype != null;
		int size = fd.getLength();
		String sqlname = sqlName(fieldname);
		String sqltype = sqlType(fieldtype, size);
		logger.trace(fieldname + " " + fieldtype + "->" + sqltype);
		if (sqltype == null) {
			String descr = sqlname + " " + fieldtype + "(" + size + ")";
			logger.warn("Field type not mapped " + descr);
			return "-- " + descr;
		}
		String result = sqlname + " " + sqltype;
		return result;
	}

	private String replaceVars(String sql, Map<String, String> vars) {
		Iterator<Map.Entry<String, String>> iterator = vars.entrySet().iterator();
		while (iterator.hasNext()) {
			Map.Entry<String, String> entry = iterator.next();
			String name = entry.getKey();
			String value = entry.getValue();
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
