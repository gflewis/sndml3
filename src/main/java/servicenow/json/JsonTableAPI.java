package servicenow.json;

import servicenow.core.*;
import servicenow.rest.JsonRecord;
import servicenow.rest.JsonResponseError;

import java.io.IOException;
import java.net.URI;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;

public class JsonTableAPI extends TableAPI {

	final URI uri;
	final CloseableHttpClient client;

	final private Logger logger = Log.logger(this.getClass());
	
	public JsonTableAPI(Table table) {
		super(table);
		String path = table.getName() + "?JSONv2";
		this.uri = session.getURI(path);
		this.client = session.getClient();
	}
	
	@Override
	public KeySet getKeys(EncodedQuery query) throws IOException {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException("not implemented");
	}

	@Override
	public Record getRecord(Key sys_id) throws IOException {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException("not implemented");
	}

	@Override
	public RecordList getRecords(String fieldname, String fieldvalue, boolean displayVaue) throws IOException {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException("not implemented");
	}

	@Override
	public RecordList getRecords(EncodedQuery query, boolean displayValue) throws IOException {
		Log.setSessionContext(session);
		Log.setTableContext(table);
		Log.setURIContext(uri);
		JSONObject requestObj = new JSONObject();
		requestObj.put("displayvalue", displayValue ? "all" : "false");
		if (query != null && !query.isEmpty()) 
			requestObj.put("sysparm_query", query.toString());
		String requestText = requestObj.toString();
		logger.debug(Log.REQUEST, requestText);
		HttpEntityEnclosingRequestBase request = new HttpPost(uri);
		HttpEntity requestEntity = new StringEntity(requestText, ContentType.APPLICATION_JSON);
		request.setEntity(requestEntity);
		CloseableHttpResponse response = client.execute(request);		
		StatusLine statusLine = response.getStatusLine();		
		int statusCode = statusLine.getStatusCode();
		HttpEntity responseEntity = response.getEntity();
		Header contentTypeHeader = responseEntity.getContentType();
		String contentType = contentTypeHeader == null ? null : contentTypeHeader.getValue();
		String responseText = EntityUtils.toString(responseEntity);
		int responseLen = responseText == null ? 0 : responseText.length();
		String responseBody = EntityUtils.toString(responseEntity);
		logger.debug(Log.RESPONSE, responseText);
		JSONObject obj;
		try {
			obj = new JSONObject(responseBody);
		}
		catch (org.json.JSONException e) {
			throw new JsonResponseError(responseBody);
		}
		response.close();
		JSONArray result = (JSONArray) obj.get("result");
		RecordList list = new RecordList(this.table, result.length());
		for (int i = 0; i < result.length(); ++i) {
			JSONObject entry = (JSONObject) result.get(i);
			JsonRecord rec = new JsonRecord(this.table, entry);
			list.add(rec);
		}
		return list;	
	}

	@Override
	public TableReader getDefaultReader() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

}
