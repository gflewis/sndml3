package servicenow.soap;

import java.io.IOException;
import java.io.StringReader;
import java.net.URI;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.jdom2.Document;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;
import org.slf4j.Logger;

import servicenow.core.*;

class XmlRequest {

	final Logger logger = Log.logger(this.getClass());

	CloseableHttpClient client;
	URI uri;
	final Document requestDoc;
	final String requestText;
	final HttpRequestBase request;
	
	public XmlRequest(CloseableHttpClient client, URI uri, Document requestDoc) {
		this.client = client;
		this.uri = uri;
		this.requestDoc = requestDoc;
		logger.debug(Log.REQUEST, uri.toString());
		// if requestDoc is null then use GET
		// this is only applicable for WSDL
		if (requestDoc == null) {
			requestText = null;
			request = new HttpGet(uri);
		}
		// otherwise use POST
		else {
			requestText = XmlFormatter.format(requestDoc);	
			HttpEntity requestEntity = new StringEntity(requestText, ContentType.TEXT_XML);
			HttpEntityEnclosingRequestBase httpPost = new HttpPost(uri);
			httpPost.setEntity(requestEntity);
			request = httpPost;
		}		
	}
	
	public Document execute() throws IOException {
		if (logger.isTraceEnabled() && requestDoc != null) 
			logger.trace(Log.REQUEST, "\n" + XmlFormatter.format(requestDoc));
		CloseableHttpResponse response = client.execute(request);		
		StatusLine statusLine = response.getStatusLine();		
		int statusCode = statusLine.getStatusCode();
		HttpEntity responseEntity = response.getEntity();
		Header contentTypeHeader = responseEntity.getContentType();
		String contentType = contentTypeHeader == null ? null : contentTypeHeader.getValue();
		String responseText = EntityUtils.toString(responseEntity);
		int responseLen = responseText == null ? 0 : responseText.length();
		String errmsg = statusLine.toString();
		logger.debug(Log.RESPONSE,
			String.format("status=\"%s\" contentType=%s len=%d", 
				statusLine, contentType, responseLen));
		if (statusCode == 401 || statusCode == 403) {
			if (requestText != null) errmsg += "\nREQUEST:\n" + requestText + "\n";
			logger.error(Log.RESPONSE, errmsg);
			throw new InsufficientRightsException(uri, requestText);
		}
		if (contentType == null) {
			logger.error(Log.RESPONSE, 
				String.format("STATUS=\"%s\"\nREQUEST:\n%s\n", statusLine, requestText));
			throw new NoContentException(uri);
		}
		// If we asked for XML and we got HTML, it must be an error page
		if ("text/html".equals(contentType))
			throw new InstanceUnavailableException(this.uri, responseText);
		SAXBuilder parser = new SAXBuilder();
		Document responseDoc = null;	
		try {
			responseDoc = parser.build(new StringReader(responseText));
		} catch (JDOMException e) {
			logger.error(Log.RESPONSE, "REQUEST:\n" + requestText + "\nRESPONSE:\n" + responseText, e);
			throw new XmlParseException(uri, e);
		}
		if (logger.isTraceEnabled()) 
			logger.trace(Log.RESPONSE, "\n" + XmlFormatter.format(responseDoc));
		return responseDoc;		
	}

}
