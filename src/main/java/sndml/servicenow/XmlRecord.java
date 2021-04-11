package sndml.servicenow;

import org.jdom2.*;

import java.util.*;


/**
 * Contains an XML document (in the form of a JDOM Element) which 
 * has been retrieved from ServiceNow.
 * The only way to obtain one of these is from a {@link SoapTableAPI} method.  
 * 
 * @author Giles Lewis
 */
public class XmlRecord extends TableRecord {
	
	final protected Element element;
	final protected Namespace ns;
	
	public XmlRecord(Table table, Element element) 
			throws SoapResponseException {
		this.table = table;
		this.element = element;
		this.ns = element.getNamespace();
	}

	@Override
	public String getValue(String fieldname) {
		String result = element.getChildText(fieldname, ns);
		if (result == null) return null;
		if (result.length() == 0) return null;
		return result;
	}	

	@Override
	public String getDisplayValue(String fieldname) {
		return getValue("dv_" + fieldname);
	}
		
	/**
	 * Return a clone of the JDOM Element underlying this object.
	 * The name of the returned element is the table name.
	 */
	Element getElement() {
		Element result = element.clone();
		result.setName(table.getName());
		return result;
	}
	
	/**
	 * The number of XML elements in this record,
	 * which may be fewer than getTable().getSchema().numFields().
	 */
	public int numFields() { 
		return element.getContentSize(); 
	}

	/**
	 * Returns the underlying JDOM Element as a formatted XML string.  
	 * Use for debugging and diagnostics.
	 * 
	 * Deprecated: Use asText()
	 */
	@Deprecated
	public String getXML() {
		return getXML(false);
	}

	/**
	 * Returns the underlying JDOM Element as a formatted XML string.  
	 * Use for debugging and diagnostics.
	 * 
	 * Deprecated: Use asText()
	 */
	@Deprecated
	public String getXML(boolean pretty) {
		return XmlFormatter.format(element, pretty);
	}
	
	@Override
	public Iterator<String> keys() {
		return getFieldNames().iterator();
	}

	
	@Override
	public FieldNames getFieldNames() {
		FieldNames result = new FieldNames();
		for (Element field : element.getChildren()) {
			result.add(field.getName());
		}
		return result;
	}
	
	public LinkedHashMap<String,String> getAllFields() {
		LinkedHashMap<String,String> result = new LinkedHashMap<String,String>();
		for (Element field : element.getChildren()) {
			String name = field.getName();
			String value = field.getText();
			result.put(name,  value);
		}
		return result;
	}

	@Override
	public String asText(boolean pretty) {
		return XmlFormatter.format(element, pretty);
	}

}
