package servicenow.rest;

import java.io.IOException;
import java.net.URI;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;

import servicenow.core.*;

public class TableImplRest extends TableImpl {

	final Table table;
	final Session session;

	final private Logger logger = Log.logger(this.getClass());
	
	public TableImplRest(Table table) {
		this.table = table;
		this.session = table.getSession();		
	}

	public Table getTable() {
		return this.table;
	}

	public Session getSession() {
		return this.session;
	}
	
	public Instance getInstance() {
		return getSession().getInstance();
	}

	public RestTableReader getDefaultReader() throws IOException {
		return new RestTableReader(this);
	}
		
	@Deprecated
	public RestTableReader getDefaultReader(EncodedQuery query, Writer writer) throws IOException {
		RestTableReader reader;
		reader = new RestTableReader(this);
		reader.setBaseQuery(query);
		reader.setWriter(writer);
		return reader;
	}

	URI getURI(Parameters params) {
		return getURI("table", null, params);
	}
	
	URI getURI(String api, Key sys_id, Parameters params) {
		assert api != null;
		String path = "api/now/" + api + "/" + table.getName();
		if (sys_id != null) path += "/" + sys_id.toString();
		URI uri = session.getURI(path, params);
		Log.setTableContext(table);
		Log.setSessionContext(session);;
		Log.setURIContext(uri);
		logger.debug(Log.REQUEST, uri.toString());
		return uri;
	}
		
	public TableStats getStats(EncodedQuery filter, boolean includeDates) throws IOException {
		TableStats stats = new TableStats();
		Parameters params = new Parameters();
		if (filter != null) params.add("sysparm_query", filter.toString());
		String aggregateFields = "sys_created_on";
		params.add("sysparm_count", "true");
		if (includeDates) {
			params.add("sysparm_min_fields", aggregateFields);
			params.add("sysparm_max_fields", aggregateFields);			
		}
		URI uri = getURI("stats", null, params);
		Log.setSessionContext(session);
		Log.setTableContext(table);
		Log.setURIContext(uri);
		HttpGet request = new HttpGet(uri);
		request.setHeader("Accept", "application/json");
		CloseableHttpResponse response = getSession().getClient().execute(request);
		String responseBody = EntityUtils.toString(response.getEntity());
		logger.debug(Log.PROCESS, "getStats\n" + responseBody);
		JSONObject obj = new JSONObject(responseBody);
		JSONObject result = obj.getJSONObject("result").getJSONObject("stats");
		stats.count = Integer.parseInt(result.getString("count"));
		if (includeDates) {
			JSONObject min = result.getJSONObject("min");
			JSONObject max = result.getJSONObject("max");
			stats.created = new DateTimeRange(
					new DateTime(min.getString("sys_created_on")),
					new DateTime(max.getString("sys_created_on")));
		}
		logger.info(Log.PROCESS, String.format("getStats query=%s count=%s", filter, stats.count));
		return stats;		
	}
	
	public Record getRecord(Key key) throws IOException {
		URI uri = getURI("table", key, null);
		JSONObject obj;
		try {
			obj = getResponseObject(uri);
		}
		catch (JsonResponseException e) {
			JSONObject err = e.obj;
			String msg = err.getString("message");
			if (msg.toLowerCase().equals("no record found")) return null;
			throw new JsonResponseError(err.toString());
		}
		JSONObject result =  (JSONObject) obj.get("result");
		if (result == null) return null;
		JsonRecord rec = new JsonRecord(this.table, result);
		return rec;
	}

	public RecordList getRecords() throws IOException {
		return getRecords((Parameters) null);
	}

	public RecordList getRecords(String fieldname, String fieldvalue, boolean displayValues) throws IOException {
		Parameters params = new Parameters();
		params.add(fieldname, fieldvalue);
		params.add("sysparm_display_value", displayValues ? "all" : "false");
		params.add("sysparm_exclude_reference_link", "true");
		return getRecords(params);
	}

	public RecordList getRecords(EncodedQuery query, boolean displayValue) throws IOException {
		Parameters params = new Parameters();
		if (query != null) params.add("sysparm_query", query.toString());
		params.add("sysparm_display_value", displayValue ? "all" : "false");
		params.add("sysparm_exclude_reference_link", "true");
		return getRecords(params);
	}
	
	public RecordList getRecords(Parameters params) throws IOException {		
		URI uri = getURI("table", null, params);
		JSONObject obj = getResponseObject(uri);
		JSONArray result = (JSONArray) obj.get("result");
		RecordList list = new RecordList(this.table, result.length());
		for (int i = 0; i < result.length(); ++i) {
			JSONObject entry = (JSONObject) result.get(i);
			JsonRecord rec = new JsonRecord(this.table, entry);
			list.add(rec);
		}
		return list;	
	}

	public KeyList getKeys(EncodedQuery filter) throws IOException {
		Parameters params = new Parameters();		
		params.add("sysparm_query", filter.toString());
		params.add("sysparm_fields", "sys_id");
		RecordList recs = getRecords(params);
		KeyList keys = recs.extractKeys();
		return keys;
	}

	private JSONObject getResponseObject(URI uri) throws IOException {
		Log.setSessionContext(session);
		Log.setTableContext(table);
		Log.setURIContext(uri);
		HttpGet request = new HttpGet(uri);
		request.setHeader("Accept", "application/json");
		JSONObject objResponse = getResponseObject(uri, request);
		if (objResponse.has("error")) {			
			throw new JsonResponseException(objResponse.getJSONObject("error"));
		}
		return objResponse;		
	}
	
	private JSONObject getResponseObject(URI uri, HttpRequestBase request) throws IOException {
		CloseableHttpResponse response = getSession().getClient().execute(request);
		HttpEntity responseEntity = response.getEntity();
		Header contentTypeHeader = responseEntity.getContentType();
		String contentType = contentTypeHeader == null ? null : contentTypeHeader.getValue();
		String responseBody = EntityUtils.toString(responseEntity);
		if ("text/html".equals(contentType) /* && responseText.contains("Hibernating") */)
			throw new InstanceUnavailableException(uri, responseBody);
		logger.trace(Log.RESPONSE, responseBody);
		JSONObject obj;
		try {
			obj = new JSONObject(responseBody);
		}
		catch (org.json.JSONException e) {
			throw new JsonResponseError(responseBody);
		}
		response.close();
		return obj;		
	}

}
