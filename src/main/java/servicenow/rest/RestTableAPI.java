package servicenow.rest;

import servicenow.core.*;

import java.io.IOException;
import java.net.URI;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.json.JSONObject;
import org.slf4j.Logger;

public class RestTableAPI extends TableAPI {

	final private Logger logger = Log.logger(this.getClass());
	
	public RestTableAPI(Table table) {
		super(table);
	}

	public Instance getInstance() {
		return getSession().getInstance();
	}
	
	private URI getURI(String api, Key sys_id, Parameters params) {
		assert api != null;
		String path = "api/now/" + api + "/" + table.getName();
		if (sys_id != null) path += "/" + sys_id.toString();
		URI uri = session.getURI(path, params);
		setAPIContext(uri);
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
		HttpGet request = new HttpGet(uri);
		request.setHeader("Accept", "application/json");
		CloseableHttpResponse response = getSession().getClient().execute(request);
		String responseBody = EntityUtils.toString(response.getEntity());
		logger.debug(Log.PROCESS, "getStats\n" + responseBody);
		JSONObject obj = new JSONObject(responseBody);
		checkForInsufficientRights(uri, obj);
		JSONObject result = obj.getJSONObject("result").getJSONObject("stats");
		stats.count = Integer.parseInt(result.getString("count"));
		if (includeDates) {
			JSONObject minValues = result.getJSONObject("min");
			JSONObject maxValues = result.getJSONObject("max");
			DateTime minCreated = new DateTime(minValues.getString("sys_created_on"));
			DateTime maxCreated = new DateTime(maxValues.getString("sys_created_on")); 
			stats.created = new DateTimeRange(minCreated, maxCreated);
		}
		logger.info(Log.PROCESS, String.format("getStats query=%s count=%s", filter, stats.count));
		return stats;		
	}
	
	public Record getRecord(Key key) throws IOException {
		URI uri = getURI("table", key, null);
		JSONObject responseObj = super.getResponseJSON(uri, HttpMethod.GET, null);
		checkForInsufficientRights(uri, responseObj);		
		if (responseObj.has("error")) {
			JSONObject errorObj = responseObj.getJSONObject("error");
			String message = errorObj.getString("message");
			if (message.equalsIgnoreCase("No record found")) return null;
			throw new JsonResponseError(responseObj.toString());
		}
		JSONObject resultObj =  responseObj.getJSONObject("result");
		if (resultObj == null) return null;
		JsonRecord rec = new JsonRecord(this.table, resultObj);
		return rec;
		/*
		JSONObject obj;
		try {
			obj = getResponseObject(uri);
		}
		catch (JsonResponseException e) {
			JSONObject err = e.getObject();
			String msg = err.getString("message");
			if (msg.toLowerCase().equals("no record found")) return null;
			throw new JsonResponseError(err.toString());
		}
		JSONObject result =  (JSONObject) obj.get("result");
		if (result == null) return null;
		JsonRecord rec = new JsonRecord(this.table, result);
		return rec;
		*/
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
		JSONObject responseObj = super.getResponseJSON(uri, HttpMethod.GET, null);
		checkForInsufficientRights(uri, responseObj);		
		assert responseObj.has("result");
		RecordList list = new RecordList(table, responseObj, "result");
		return list;
	}

	public Key insertRecord(Parameters fields) throws IOException {
		URI uri = getURI("table", null, null);
		JSONObject requestObj = fields.toJSON();
		JSONObject responseObj = super.getResponseJSON(uri, HttpMethod.PUT, requestObj);
		checkForInsufficientRights(uri, responseObj);		
		assert responseObj.has("result");
		JSONObject resultObj = responseObj.getJSONObject("result");
		String sys_id = resultObj.getString("sys_id");
		Key key = new Key(sys_id);
		return key;
	}

	/*
	private JSONObject getResponseObject(URI uri) throws IOException {
		setAPIContext(uri);
		HttpGet request = new HttpGet(uri);
		request.setHeader("Accept", "application/json");
		return super.getResponseJSON(request,  null);
		JSONObject objResponse = getResponseObject(uri, request);
		checkResponseJSON(uri, objResponse);
		return objResponse;
	}

	/*
	private JSONObject getResponseObject(URI uri, HttpRequestBase request) throws IOException {
		CloseableHttpResponse response = getSession().getClient().execute(request);
		HttpEntity responseEntity = response.getEntity();
		Header contentTypeHeader = responseEntity.getContentType();
		String contentType = contentTypeHeader == null ? null : contentTypeHeader.getValue();
		String responseBody = EntityUtils.toString(responseEntity);
		if ("text/html".equals(contentType))
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
	*/

	@Override
	public RestTableReader getDefaultReader() throws IOException {
		return new RestTableReader(this.table);
	}
	
}
