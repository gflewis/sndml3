package sndml.servicenow;

import java.io.IOException;
import java.net.URI;
import org.slf4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class RestTableAPI extends TableAPI {

	final private Logger logger = Log.logger(this.getClass());
	
	public RestTableAPI(Table table) {
		super(table);
	}

	public Instance getInstance() {
		return getSession().getInstance();
	}
	
	private URI getURI(String api, RecordKey sys_id, Parameters params) {
		assert api != null;
		String path = "api/now/" + api + "/" + table.getName();
		if (sys_id != null) path += "/" + sys_id.toString();
		URI uri = session.getURI(path, params);
		return uri;
	}
		
	public TableStats getStats(EncodedQuery filter, boolean includeDates) throws IOException {
		Log.setMethodContext(table, "STATS");
		TableStats tableStats = new TableStats();
		Parameters params = new Parameters();
		if (filter != null && !filter.isEmpty()) params.add("sysparm_query", filter.toString());
		String aggregateFields = "sys_created_on";
		params.add("sysparm_count", "true");
		if (includeDates) {
			params.add("sysparm_min_fields", aggregateFields);
			params.add("sysparm_max_fields", aggregateFields);			
		}
		URI uri = getURI("stats", null, params);
		JsonRequest request = new JsonRequest(session, uri, HttpMethod.GET, null);
		ObjectNode root = request.execute();
		if (logger.isDebugEnabled()) logger.debug(Log.PROCESS, request.dumpResponseText());
		request.checkForInsufficientRights();
		// ObjectNode result = (ObjectNode) root.get("result");
		// ObjectNode stats = (ObjectNode) result.get("stats");
		tableStats.count = root.at("/result/stats/count").asInt();
		if (includeDates) {
			JsonNode minValues = root.at("/result/stats/min");
			JsonNode maxValues = root.at("/result/stats/max");
			DateTime minCreated = DateTime.from(minValues.get("sys_created_on").asText());
			DateTime maxCreated = DateTime.from(maxValues.get("sys_created_on").asText());
			if (minCreated == null || maxCreated == null) 
				logger.warn(Log.PROCESS, String.format(
					"getStats minCreated=%s maxCreated=%s query=%s", 
					minCreated, maxCreated, filter));
			tableStats.created = new DateTimeRange(minCreated, maxCreated);
			logger.info(Log.PROCESS, String.format(
				"getStats count=%d createdRange=%s query=%s", 
				tableStats.count, tableStats.created, filter));	
		}
		else {
			logger.info(Log.PROCESS, String.format(
				"getStats count=%d query=%s", tableStats.count, filter));			
		}
		return tableStats;		
	}
	
	public TableRecord getRecord(RecordKey key) throws IOException {
		Log.setMethodContext(table, "GET");
		URI uri = getURI("table", key, null);
		JsonRequest request = new JsonRequest(session, uri, HttpMethod.GET, null);
		ObjectNode responseObj = request.execute();
		request.checkForInsufficientRights();		
		if (responseObj.has("error")) {
			JsonNode errorObj = responseObj.get("error");
			String message = errorObj.get("message").asText();
			if (message.equalsIgnoreCase("No record found")) return null;
			throw new JsonResponseError(responseObj.toString());
		}
		ObjectNode resultObj = (ObjectNode) responseObj.get("result");
		if (resultObj == null) return null;
		JsonRecord rec = new JsonRecord(this.table, resultObj);
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
		Log.setMethodContext(table, "GET");
		URI uri = getURI("table", null, params);
		JsonRequest request = new JsonRequest(session, uri, HttpMethod.GET, null);
		ObjectNode root = request.execute();
		request.checkForInsufficientRights();
		ArrayNode resultObj = (ArrayNode) root.get("result");
		RecordList list = new RecordList(table, resultObj);
		return list;
	}

	public InsertResponse insertRecord(Parameters fields) throws IOException {
		Log.setMethodContext(table, "POST");
		URI uri = getURI("table", null, null);
		ObjectNode requestObj = fields.toJSON();
		JsonRequest request = new JsonRequest(session, uri, HttpMethod.POST, requestObj);
		ObjectNode root = request.execute();
		request.checkForInsufficientRights();
		ObjectNode resultObj = (ObjectNode) root.get("result");
		assert root.has("result");
		JsonRecord rec = new JsonRecord(this.table, resultObj);
		return rec;
	}

	public void updateRecord(RecordKey key, Parameters fields) throws IOException {
		Log.setMethodContext(table, "PUT");
		URI uri = getURI("table", key, null);
		ObjectNode requestObj = fields.toJSON();
		JsonRequest request = new JsonRequest(session, uri, HttpMethod.PUT, requestObj);
		ObjectNode root = request.execute();
		request.checkForInsufficientRights();
		request.checkForNoSuchRecord();
		ObjectNode resultObj = (ObjectNode) root.get("result");
		@SuppressWarnings("unused")
		JsonRecord rec = new JsonRecord(this.table, resultObj);
	}
	
	public boolean deleteRecord(RecordKey key) throws IOException {
		Log.setMethodContext(table, "DELETE");
		URI uri = getURI("table", key, null);
		JsonRequest request = new JsonRequest(session, uri, HttpMethod.DELETE, null);
		ObjectNode root = request.execute();
		if (root == null) return true;
		if (request.recordNotFound()) return false;
		throw new JsonResponseException(request);
	}
	
	@Override
	public RestTableReader getDefaultReader() throws IOException {
		return new RestTableReader(this.table);
	}
	
}
