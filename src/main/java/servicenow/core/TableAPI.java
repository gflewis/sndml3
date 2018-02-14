package servicenow.core;

import java.io.IOException;
import java.net.URI;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import servicenow.soap.SoapResponseException;

public abstract class TableAPI {

	final protected Table table;
	final protected Session session;
	
	final private Logger logger = LoggerFactory.getLogger(this.getClass());
	
	public TableAPI(Table table) {
		this.table = table;
		this.session = table.getSession();
	}

	public Table getTable() {
		return this.table;
	}

	public Session getSession() {
		return this.session;
	}

	public String getTableName() {
		return table.getName();
	}

	@Deprecated
	protected void setAPIContext(URI uri) {
		Log.setSessionContext(session);
		Log.setTableContext(table);
		Log.setURIContext(uri);		
	}
	
	/**
	 * Gets a record using the sys_id. 
	 * If the record is not found then null is returned.
	 * 
	 * @param sys_id
	 * @return Record if found otherwise null.
	 * @throws IOException
	 */
 	public abstract Record getRecord(Key sys_id) throws IOException;
 	
 	public abstract RecordList getRecords(EncodedQuery query, boolean displayValue) throws IOException;
 	
 	public abstract InsertResponse insertRecord(Parameters fields) throws IOException;
 	
 	public abstract void updateRecord(Key key, Parameters fields) throws IOException;
 	
 	public abstract boolean deleteRecord(Key key) throws IOException;

 	public abstract TableReader getDefaultReader() throws IOException;

 	public RecordList getRecords() throws IOException {
 		return getRecords(false);
 	}
 	
 	public RecordList getRecords(boolean displayValue) throws IOException {
 		return getRecords((EncodedQuery) null, displayValue);
 	}
 	
 	public RecordList getRecords(String fieldname, String fieldvalue) throws IOException {
 		return getRecords(fieldname, fieldvalue, false);
 	}
 	
 	public RecordList getRecords(EncodedQuery query) throws IOException {
 		return getRecords(query, false);
 	}
 	
	public RecordList getRecords(String fieldname, String fieldvalue, boolean displayValue) throws IOException {
		EncodedQuery query = new EncodedQuery(fieldname, fieldvalue);
		return getRecords(query, displayValue);
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
		String msg = String.format("get %s=%s returned %d records", fieldname, fieldvalue, size);
		logger.info(Log.RESPONSE, msg);
		if (size == 0) return null;
		if (size > 1) throw new RowCountExceededException(getTable(), msg);
		return result.get(0);
	}

	protected JSONObject getResponseJSON(URI uri, HttpMethod method, JSONObject requestObj) throws IOException {
		String requestText = null;
		if (requestObj != null) {
			requestText = requestObj.toString();
		}		
		String responseText = getResponseText(uri, method, requestText);
		if (responseText == null) {
			// Success - No Content
			return null;
		}
		JSONObject responseObj;
		try {
			responseObj = new JSONObject(responseText);
		}
		catch (org.json.JSONException e) {
			throw new JsonResponseError(responseText);
		}
		if (responseObj.has("error")) {
			logger.warn(Log.RESPONSE, responseObj.toString());
		}
		return responseObj;		
	}
	
	protected String getResponseText(URI uri, HttpMethod method, String requestText) throws IOException {		
		Log.setSessionContext(session);
		Log.setTableContext(table);
		Log.setURIContext(uri);		
		HttpUriRequest request;
		HttpEntity requestEntity = null;
		logger.debug(Log.REQUEST, method.name() + " " + uri.toURL());
		if (requestText != null) {
			if (logger.isTraceEnabled()) logger.trace(Log.REQUEST, requestText);
			requestEntity = new StringEntity(requestText, ContentType.APPLICATION_JSON);
		}
		switch (method) {
		case DELETE:
			assert requestText == null;
			HttpDelete httpDelete = new HttpDelete(uri);
			request = httpDelete;
			break;
		case GET:
			assert requestText == null;
			HttpGet httpGet = new HttpGet(uri);
			request = httpGet;
			break;
		case PATCH:
			assert requestText != null;
			HttpPatch httpPatch = new HttpPatch(uri);
			httpPatch.setEntity(requestEntity);
			httpPatch.setHeader("Content-Type", "application/json");
			request = httpPatch;
			break;
		case POST:
			assert requestText != null;
			HttpPost httpPost = new HttpPost(uri);
			httpPost.setEntity(requestEntity);
			httpPost.setHeader("Content-Type", "application/json");
			request = httpPost;
			break;
		case PUT:
			assert requestText != null;
			HttpPut httpPut = new HttpPut(uri);
			httpPut.setEntity(requestEntity);
			httpPut.setHeader("Content-Type", "application/json");
			request = httpPut;
			break;
		default:
			throw new AssertionError();
		}
		request.setHeader("Accept", "application/json");
		CloseableHttpResponse response = session.getClient().execute(request);		
		StatusLine statusLine = response.getStatusLine();		
		int statusCode = statusLine.getStatusCode();
		HttpEntity responseEntity = response.getEntity();
		String contentType = null;
		String responseText = null;
		if (responseEntity != null) {
			Header contentTypeHeader = responseEntity.getContentType();
			if (contentTypeHeader != null) contentType = contentTypeHeader.getValue();
			responseText = EntityUtils.toString(responseEntity);			
		}
		response.close();
		int responseLen = responseText == null ? 0 : responseText.length();
		logger.debug(Log.RESPONSE,
				String.format("status=\"%s\" contentType=%s len=%d", 
					statusLine, contentType, responseLen));
		logger.trace(Log.RESPONSE, responseText);
		if (statusCode == 204) {
			// Success - No Content
			return null;
		}
		if (statusCode == 401 || statusCode == 403) {
			logger.error(Log.REQUEST, Log.join(uri, requestText));
			logger.error(Log.RESPONSE, statusLine.toString());
			throw new InsufficientRightsException(uri, requestText);
		}
		if (contentType == null) {
			logger.error(Log.REQUEST, Log.join(uri, requestText));
			logger.error(Log.RESPONSE, statusLine.toString());
			throw new NoContentException(uri, requestText);
		}		
		if ("text/html".equals(contentType))
			throw new InstanceUnavailableException(request.getURI(), responseText);		
		return responseText;
	}
	
	static protected String errorMessageLowerCase(JSONObject objResponse) {
		if (!objResponse.has("error")) return null;
		JSONObject error = objResponse.getJSONObject("error");
		if (!error.has("message")) return null;
		return error.getString("message").toLowerCase();
	}
		
	protected void checkForInsufficientRights(URI uri, JSONObject objResponse) throws IOException {
		String err = errorMessageLowerCase(objResponse);
		if (err == null) return;
		if (err.contains("not authorized") ||
				err.contains("insufficient rights") ||
				err.contains("no permission"))
			throw new InsufficientRightsException(uri);
	}
	
}
