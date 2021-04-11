package sndml.servicenow;

import java.io.IOException;
import java.util.Iterator;
import org.jdom2.Element;
import org.jdom2.Namespace;
import org.slf4j.Logger;

public class SoapTableAPI extends TableAPI {

	final SoapClient client;
	TableWSDL wsdl = null;
	
	final Logger logger = Log.logger(this.getClass());
	
	public SoapTableAPI(Table table) {
		super(table);
		this.client = new SoapClient(table.getSession(), table.getName());
	}
	
	public TableWSDL getWSDL() throws IOException {
		if (wsdl==null) {
			Log.setTableContext(this.table, getTableName() + ".WSDL");
			Log.setMethodContext(table, "WSDL");					
			wsdl = new TableWSDL(this.session, getTableName());
		}
		return wsdl;
	}

	public KeySet getKeys() throws IOException {
		return getKeys((Parameters) null);
	}
	
	public KeySet getKeys(EncodedQuery query) throws IOException {
		Parameters params = new Parameters();
		if (query != null) params.add("__encoded_query", query.toString());
		params.add("__order_by", "sys_id");
		return getKeys(params);
	}
	
	/**
	 * Return a list of all the keys in the table
	 * with extended query parameters. 
	 * 
	 * This method is called by {@link KeySetTableReader}.
	 */
	public KeySet getKeys(Parameters params) throws IOException {
		Log.setMethodContext(table, "getKeys");
		Element responseElement = client.executeRequest("getKeys", params, null, "getKeysResponse");
		Namespace ns = responseElement.getNamespace();
		int size = Integer.parseInt(responseElement.getChildText("count", ns));
		logger.trace(Log.RESPONSE, "getKeys returned " + size + " keys");
		KeySet result = new KeySet();
		if (size > 0) {
			result.ensureCapacity(size);
			String listStr = responseElement.getChildText("sys_id", ns);
			String list[] = listStr.split(",");
			if (list.length != size) {
				logger.error(Log.RESPONSE, XmlFormatter.format(responseElement));
				throw new SoapResponseException(this.table, "getKeys",
					"expected: " + size + ", found=" + list.length, responseElement);
			}
			for (int i = 0; i < list.length; ++i) {
				result.add(new RecordKey(list[i]));
			}
		}
		return result;		
	}
	
	public TableRecord getRecord(RecordKey key) throws IOException {
		Log.setMethodContext(table, "get");
		Parameters params = new Parameters("sys_id", key.toString());
		Element responseElement = client.executeRequest("get", params, null, "getResponse");
		if (responseElement.getContentSize() == 0) return null;
		TableRecord rec = new XmlRecord(getTable(), responseElement);
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
	 * {@link TableRecord} grouprec = session.table("sys_user_group").get("name", "Network Support");
	 * </pre>
	 * 
	 * @param fieldname Field name, e.g. "number" or "name"
	 * @param fieldvalue Field value
	 */
	public TableRecord getRecord(String fieldname, String fieldvalue, boolean displayValues)
			throws IOException, SoapResponseException {
		RecordList result = getRecords(fieldname, fieldvalue, displayValues);
		int size = result.size();
		String msg = String.format("get %s=%s returned %d records", fieldname, fieldvalue,size);
		logger.info(Log.RESPONSE, msg);
		if (size == 0) return null;
		if (size > 1) throw new TooManyRowsException(getTable(), 1, size); 
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
		Log.setMethodContext(table, "getRecords");
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

	public InsertResponse insertRecord(Parameters docParams) throws IOException {
		Log.setMethodContext(table, "insert");
		Element responseElement = 
			client.executeRequest("insert", docParams, null, "insertResponse");
		TableRecord rec = new XmlRecord(getTable(), responseElement);
		return rec;
	}

	public void updateRecord(RecordKey key, Parameters fields) throws IOException {
		Log.setMethodContext(table, "update");
		Parameters docParams = new Parameters();
		docParams.add(fields);
		docParams.add("sys_id", key.toString());
		@SuppressWarnings("unused")
		Element responseElement =
			client.executeRequest("update", docParams, null, "updateResponse");
	}
	
	public boolean deleteRecord(RecordKey key) throws IOException {
		Log.setMethodContext(table, "deleteRecord");
		Parameters docParams = new Parameters("sys_id", key.toString());
		Element responseElement =
			client.executeRequest("deleteRecord", docParams, null, "deleteRecordResponse");
		Element countElement = responseElement.getChild("count");
		int count = Integer.parseInt(countElement.getText());
		if (count == 0) return false;
		if (count == 1) return true;
		logger.error(Log.RESPONSE, XmlFormatter.format(responseElement));
		throw new SoapResponseException(this.table, "deleteRecord", 
				"element \"count\" not found", responseElement);
	}

	@Deprecated
	public TableReader getDefaultReader() throws IOException {
		return new SoapKeySetTableReader(this.table);
	}

}

		