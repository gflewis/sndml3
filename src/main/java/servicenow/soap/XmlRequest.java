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

public class XmlRequest {

	final Logger log = Log.logger(this.getClass());

	CloseableHttpClient client;
	URI uri;
	final String requestText;
	final HttpRequestBase request;
	
	public XmlRequest(CloseableHttpClient client, URI uri, Document requestDoc) {
		this.client = client;
		this.uri = uri;
		log.debug(Log.REQUEST, uri.toString());
		// if requestDoc is null then use GET
		// this is only applicable for WSDL
		if (requestDoc == null) {
			requestText = null;
			request = new HttpGet(uri);
		}
		// otherwise use POST
		else {
			requestText = XmlFormatter.format(requestDoc);	
			log.trace(Log.REQUEST, requestText);			
			HttpEntity requestEntity = new StringEntity(requestText, ContentType.TEXT_XML);
			HttpEntityEnclosingRequestBase httpPost = new HttpPost(uri);
			httpPost.setEntity(requestEntity);
			request = httpPost;
		}		
	}
	
	public Document execute() throws IOException {
		CloseableHttpResponse response = client.execute(request);		
		StatusLine statusLine = response.getStatusLine();		
		int statusCode = statusLine.getStatusCode();
		HttpEntity responseEntity = response.getEntity();
		Header contentTypeHeader = responseEntity.getContentType();
		String contentType = contentTypeHeader == null ? null : contentTypeHeader.getValue();
		String responseText = EntityUtils.toString(responseEntity);
		int responseLen = responseText == null ? 0 : responseText.length();
		log.debug(Log.RESPONSE,
			String.format("status=\"%s\" contentType=%s len=%d", 
				statusLine, contentType, responseLen));
		if (statusCode == 401) {
			log.error(Log.RESPONSE, 
				String.format("STATUS=\"%s\"\nREQUEST:\n%s\n", statusLine, requestText));
			throw new InsufficientRightsException(uri, null, requestText);
		}
		if (contentType == null) {
			log.error(Log.RESPONSE, 
				String.format("STATUS=\"%s\"\nREQUEST:\n%s\n", statusLine, requestText));
			throw new NoContentException(uri);
		}		
		if ("text/html".equals(contentType) /* && responseText.contains("Hibernating") */)
			throw new InstanceUnavailableException(this.uri, responseText);
		SAXBuilder parser = new SAXBuilder();
		Document responseDoc = null;	
		try {
			responseDoc = parser.build(new StringReader(responseText));
		} catch (JDOMException e) {
			log.error(Log.RESPONSE, "REQUEST:\n" + requestText + "\nRESPONSE:\n" + responseText, e);
			throw new XmlParseException(uri, e);
		}
		if (log.isTraceEnabled()) log.trace(Log.RESPONSE, XmlFormatter.format(responseDoc));
		return responseDoc;		
	}

}
