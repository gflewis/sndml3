package servicenow.soap;

import org.jdom2.*;

import servicenow.core.*;

import java.util.*;


/**
 * Contains an XML document (in the form of a JDOM Element) which 
 * has been retrieved from ServiceNow.
 * The only way to obtain one of these is to use 
 * {@link Table#get(Key) Table.get()} or
 * {@link Table#getRecords(QueryFilter) Table.getRecords()}. 
 * 
 * @author Giles Lewis
 */
public class XmlRecord extends Record {
	
	final protected Element element;
	final protected Namespace ns;
	
	protected XmlRecord(Table table, Element element) 
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
	 */
	public String getXML() {
		return getXML(false);
	}

	/**
	 * Returns the underlying JDOM Element as a formatted XML string.  
	 * Use for debugging and diagnostics.
	 */
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


}
