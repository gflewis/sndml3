package sndml.servicenow;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.Namespace;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public class TableWSDL {

	final private Logger logger = LoggerFactory.getLogger(this.getClass());

	final String tablename;
	final URI uri;
	final private Document doc;
	final FieldNames readColumnNames;
	final FieldNames writeColumnNames;
	final Map<String, String> readColumnTypes;
	final Map<String, String> writeColumnTypes;

	static Namespace nsWSDL = Namespace.getNamespace("wsdl", "http://schemas.xmlsoap.org/wsdl/");
	static Namespace nsXSD = Namespace.getNamespace("xsd", "http://www.w3.org/2001/XMLSchema");
	
	public TableWSDL(Session session, String tablename) throws IOException {
		this(session, tablename, false);
	}

	TableWSDL(Session session, String tablename, boolean displayvalues) throws IOException {
		this.tablename = tablename;
		String path = tablename + ".do?WSDL";
		if (displayvalues) path += "&displayvalue=all";
		uri = session.getURI(path);
		Log.setURIContext(uri);
		logger.debug(Log.WSDL, uri.toString());

		XmlRequest request = new XmlRequest(session.getClient(), uri, null);
		try {
			doc = request.getDocument();
		} catch (NoContentException e) {
			throw new InvalidTableNameException(tablename);
		}
		if (logger.isTraceEnabled()) 
			logger.trace(Log.WSDL, "\n" + XmlFormatter.format(doc));

		readColumnNames = getColumnNames("getResponse");
		readColumnTypes = getColumnTypes("getResponse");
		writeColumnNames = getColumnNames("update");
		writeColumnTypes = getColumnTypes("update");
		Log.clearURIContext();
	}

	Document getDocument() {
		return this.doc;
	}

	private Element getTypeDefinition(String typeName) throws WSDLException {
		List<Element> eleElements = doc.getRootElement()
				.getChild("types", nsWSDL)
				.getChild("schema", nsXSD)
				.getChildren("element", nsXSD);
		Element getResponse = null;
		for (Element ele : eleElements) {
			String name = ele.getAttributeValue("name");
			if (name.equals(typeName))
				getResponse = ele;
		}
		if (getResponse == null)
			throw new WSDLException("Could not find \"" + typeName + "\" in WSDL");
		Element complexType = getResponse.getChild("complexType", nsXSD);
		Element sequence = complexType.getChild("sequence", nsXSD);
		return sequence;
	}

	private FieldNames getColumnNames(String typeName) throws WSDLException {
		Element schema = getTypeDefinition(typeName);
		List<Element> children = schema.getChildren("element", nsXSD);
		FieldNames list = new FieldNames(children.size());
		for (Element child : children) {
			list.add(child.getAttributeValue("name"));
		}
		return list;
	}

	private Map<String, String> getColumnTypes(String typeName) throws WSDLException {
		Element schema = getTypeDefinition(typeName);
		List<Element> children = schema.getChildren("element", nsXSD);
		HashMap<String, String> map = new HashMap<String, String>(children.size());
		for (Element child : children) {
			String name = child.getAttributeValue("name");
			String type = child.getAttributeValue("type");
			type.replaceFirst("xsd:", "");
			map.put(name, type);
		}
		return map;
	}

	/**
	 * Return a list of all the columns in the table.
	 */
	public FieldNames getReadFieldNames() {
		return readColumnNames;
	}

	public FieldNames getWriteFieldNames() {
		return writeColumnNames;
	}

	public boolean canReadField(String fieldname) {
		return readColumnTypes.get(fieldname) != null;
	}

	public boolean canWriteField(String fieldname) {
		return writeColumnTypes.get(fieldname) != null;
	}

	/*
	 * Return the list of fields that are NOT in the provided list
	 */
	public FieldNames invert(FieldNames include) {
		FieldNames exclude = new FieldNames();
		for (String name : this.readColumnNames) {
			if (!include.contains(name))
				exclude.add(name);
		}
		return exclude;
	}

}
