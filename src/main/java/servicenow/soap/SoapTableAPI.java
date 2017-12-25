package servicenow.soap;

import java.io.IOException;
import java.util.Iterator;

import org.jdom2.Element;
import org.jdom2.Namespace;
import org.slf4j.Logger;

import servicenow.core.*;

public class SoapTableAPI extends TableAPI {

	final Table table;
	final String tablename;
	final Session session;
	final SoapClient client;
	TableWSDL wsdl = null;
	
	final Logger log = Log.logger(this.getClass());
	
	public SoapTableAPI(Table table) {
		this.table = table;
		this.tablename = table.getName();
		this.session = table.getSession();
		this.client = new SoapClient(table.getSession(), table.getName());
	}

	public Table getTable() {
		return this.table;
	}
	
	public String getName() {
		return table.getName();
	}
	
	public Session getSession() {
		return this.session;
	}

	public TableWSDL getWSDL() throws IOException {
		if (wsdl==null) wsdl = new TableWSDL(this.session, this.tablename);
		return wsdl;
	}

	public TableReader getDefaultReader() throws IOException {
		return new SoapTableReader(this);
	}
			
	public KeyList getKeys() throws IOException {
		return getKeys((Parameters) null);
	}
	
	public KeyList getKeys(EncodedQuery filter) throws IOException {
		Parameters params = new Parameters();
		if (filter != null) params.add("__encoded_query", filter.toString());
		return getKeys(params);
	}
	
	/**
	 * Return a list of all the keys in the table
	 * with extended query parameters. 
	 * 
	 * This method is called by {@link KeyReader}.
	 */
	public KeyList getKeys(Parameters params) throws IOException {
		Element responseElement = client.executeRequest("getKeys", params, null, "getKeysResponse");
		Namespace ns = responseElement.getNamespace();
		int size = Integer.parseInt(responseElement.getChildText("count", ns));
		log.trace(Log.RESPONSE, "getKeys returned " + size + " keys");
		KeyList result = new KeyList();
		if (size > 0) {
			result.ensureCapacity(size);
			String listStr = responseElement.getChildText("sys_id", ns);
			String list[] = listStr.split(",");
			if (list.length != size)
				throw new SoapResponseException(this.table, 
					"getKeys expected: " + size + ", found=" + list.length + "\n" +
					XmlFormatter.format(responseElement));
			for (int i = 0; i < list.length; ++i) {
				result.add(new Key(list[i]));
			}
		}
		return result;		
	}
	
	public Record getRecord(Key key) throws IOException {
		Parameters params = new Parameters("sys_id", key.toString());
		Element responseElement = client.executeRequest("get", params, null, "getResponse");
		Record rec = new XmlRecord(getTable(), responseElement);
		return rec;		
	}
	
	/**
	 * Retrieves a single record based on a unique field such as "name" or "number".  
	 * This method should be used in cases where the field value is known to be unique.
	 * If no qualifying records are found this function will return null.
	 * If one qualifying record is found it will be returned.
	 * If multiple qualifying records are found this method 
	 * will throw an RowCountExceededException.
	 * <pre>
	 * {@link Record} grouprec = session.table("sys_user_group").get("name", "Network Support");
	 * </pre>
	 * 
	 * @param fieldname Field name, e.g. "number" or "name"
	 * @param fieldvalue Field value
	 */
	public Record getRecord(String fieldname, String fieldvalue, boolean displayValues)
			throws IOException, SoapResponseException {
		RecordList result = getRecords(fieldname, fieldvalue, displayValues);
		int size = result.size();
		String msg = String.format("get %s=%s returned %d records", fieldname, fieldvalue,size);
		log.info(Log.RESPONSE, msg);
		if (size == 0) return null;
		if (size > 1) throw new RowCountExceededException(getTable(), msg);
		return result.get(0);
	}
	
	public RecordList getRecords(String fieldname, String fieldvalue, boolean displayValue) throws IOException {
		Parameters params = new Parameters();
		params.add(fieldname, fieldvalue);
		return getRecords(params, displayValue);		
	}
	
	public RecordList getRecords(EncodedQuery filter, boolean displayValue) throws IOException {
		Parameters params = new Parameters();
		if (filter != null) params.add("__encoded_query", filter.toString());
		return getRecords(params, displayValue);
	}
	
	/**
	 * It calls the SOAP getRecords method and allows specification of a list of
	 * parameters and extended query parameters.
	 * @see <a href="http://wiki.servicenow.com/index.php?title=Direct_Web_Service_API_Functions#getRecords">http://wiki.servicenow.com/index.php?title=Direct_Web_Service_API_Functions#getRecords</a>
	 */
	
	public RecordList getRecords(Parameters docParams, boolean displayValue) throws IOException {
		Parameters uriParams = new Parameters();
		uriParams.add("displayvalue", displayValue ? "all" : "false");
		Element responseElement = 
			client.executeRequest("getRecords", docParams, uriParams, "getRecordsResponse");
		int size = responseElement.getContentSize();
		RecordList list = new RecordList(getTable(), size);
		Iterator<Element> iter = responseElement.getChildren().iterator();
		while (iter.hasNext()) {
			Element next = iter.next();
			XmlRecord rec = new XmlRecord(this.table, next);
			list.add(rec);
		}
		return list;		
	}

}
