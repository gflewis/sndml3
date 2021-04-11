package sndml.servicenow;

import java.io.IOException;
import java.net.URI;
import org.slf4j.Logger;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class JsonTableAPI extends TableAPI {

	final URI uri;

	final private Logger logger = Log.logger(this.getClass());
	
	public JsonTableAPI(Table table) {
		super(table);
		String path = table.getName() + ".do?JSONv2";
		this.uri = session.getURI(path);
		logger.debug(Log.INIT, this.uri.toString());
	}

	public KeySet getKeys() throws IOException {
		return getKeys(null);
	}
		
	public KeySet getKeys(EncodedQuery query) throws IOException {
		Log.setMethodContext(table, "getKeys");
		Parameters params = new Parameters();
		params.put("sysparm_action",  "getKeys");
		if (!EncodedQuery.isEmpty(query)) params.put("sysparm_query", query.toString());
		ObjectNode requestObj = params.toJSON();
		JsonRequest request = new JsonRequest(session, uri, HttpMethod.POST, requestObj);		
		ObjectNode responseObj = request.execute();
		ArrayNode recordsObj = (ArrayNode) responseObj.get("records");
		KeySet keys = new KeySet(recordsObj);
		return keys;
	}

	public TableRecord getRecord(RecordKey sys_id) throws IOException {
		Log.setMethodContext(table, "get");
		Parameters params = new Parameters();
		params.add("sysparm_action", "get");
		params.add("sysparm_sys_id",  sys_id.toString());
		ObjectNode requestObj = params.toJSON();
		JsonRequest request = new JsonRequest(session, uri, HttpMethod.POST, requestObj);
		ObjectNode responseObj = request.execute();
		assert responseObj.has("records");
		assert responseObj.get("records").isArray();
		ArrayNode recordsObj = (ArrayNode) responseObj.get("records");
		RecordList recs = new RecordList(table, recordsObj);
		assert recs != null;
		if (recs.size() == 0) return null;
		return recs.get(0);
	}

	public RecordList getRecords(KeySet keys, boolean displayValue) throws IOException {
		EncodedQuery query = new EncodedQuery(table, keys);
		return getRecords(query, displayValue);
	}
	
	public RecordList getRecords(EncodedQuery query, boolean displayValue) throws IOException {
		Parameters params = new Parameters();
		params.add("displayvalue", displayValue ? "all" : "false");
		if (!EncodedQuery.isEmpty(query))
			params.add("sysparm_query", query.toString());
		return getRecords(params);
	}

	public RecordList getRecords(Parameters params) throws IOException {
		Log.setMethodContext(table, "getRecords");
		ObjectNode requestObj = params.toJSON();
		requestObj.put("sysparm_action", "getRecords");
		JsonRequest request = new JsonRequest(session, uri, HttpMethod.POST, requestObj);
		ObjectNode responseObj = request.execute();
		assert responseObj.has("records");
		assert responseObj.get("records").isArray();
		ArrayNode recordsObj = (ArrayNode) responseObj.get("records");
		return new RecordList(table, recordsObj);
	}
		
	public InsertResponse insertRecord(Parameters fields) throws IOException {
		throw new UnsupportedOperationException();
		/*
		Log.setMethodContext(table, "insert");
		JSONObject requestObj = new JSONObject();
		requestObj.put("sysparm_action", "insert");
		Parameters.appendToObject(fields, requestObj);
		JsonRequest request = new JsonRequest(client, uri, HttpMethod.POST, requestObj);
		JSONObject responseObj = request.execute();
		RecordList list = new RecordList(table, responseObj, "records");
		assert list.size() > 0;
		assert list.size() == 1;
		return list.get(0);
		*/
	}
	
	public void updateRecord(RecordKey key, Parameters fields) throws IOException {
		throw new UnsupportedOperationException();
		/*
		Log.setMethodContext(table, "update");
		JSONObject requestObj = new JSONObject();
		requestObj.put("sysparm_action", "update");
		requestObj.put("sysparm_sys_id",  key.toString());
		Parameters.appendToObject(fields, requestObj);
		JsonRequest request = new JsonRequest(client, uri, HttpMethod.POST, requestObj);
		JSONObject responseObj = request.execute();
		RecordList list = new RecordList(table, responseObj, "records");
		if (list.size() != 1) throw new JsonResponseException(request);
		*/
	}

	public boolean deleteRecord(RecordKey key) throws IOException {
		throw new UnsupportedOperationException();
		/*
		Log.setMethodContext(table, "deleteRecord");
		JSONObject requestObj = new JSONObject(); 
		requestObj.put("sysparm_action", "deleteRecord");
		requestObj.put("sysparm_sys_id",  key.toString());
		JsonRequest request = new JsonRequest(client, uri, HttpMethod.POST, requestObj);
		JSONObject responseObj = request.execute();
		RecordList list = new RecordList(table, responseObj, "records");
		if (list.size() == 0) return false;
		if (list.size() > 1) throw new JsonResponseException(request);
		if (list.get(0).getKey().equals(key)) return true;
		throw new JsonResponseException(request);
		*/
	}

	@Override
	public TableReader getDefaultReader() throws IOException {
		return new KeySetTableReader(this.table);
	}

}
