package sndml.servicenow;

import java.io.IOException;
import java.io.StringReader;
import java.net.URI;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.jdom2.Document;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;
import org.slf4j.Logger;

class XmlRequest extends ServiceNowRequest {

	final Logger logger = Log.logger(this.getClass());

	final Document requestDoc;
	final HttpUriRequest request;
	
	public XmlRequest(CloseableHttpClient client, URI uri, Document requestDoc) {
		super(client, uri, getMethod(requestDoc));
		this.requestDoc = requestDoc;
		logger.debug(Log.REQUEST, uri.toString());
		// if requestDoc is null then use GET
		// this is only applicable for WSDL
		if (method == HttpMethod.GET) {
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
	
	private static HttpMethod getMethod(Document requestDoc) {
		if (requestDoc == null) 
			return HttpMethod.GET;
		else 
			return HttpMethod.POST;
	}
	
	public Document getDocument() throws IOException {
		CloseableHttpResponse response = client.execute(request);		
		statusLine = response.getStatusLine();		
		statusCode = statusLine.getStatusCode();
		HttpEntity responseEntity = response.getEntity();
		Header contentTypeHeader = responseEntity.getContentType();
		responseContentType = contentTypeHeader == null ? null : contentTypeHeader.getValue();
		responseText = EntityUtils.toString(responseEntity);
		int responseLen = responseText == null ? 0 : responseText.length();
		logger.debug(Log.RESPONSE,
			String.format("status=\"%s\" contentType=%s len=%d", 
				statusLine, responseContentType, responseLen));
		if (statusCode == 401 || statusCode == 403) {
			logger.error(Log.RESPONSE, this.dump());
			throw new InsufficientRightsException(this);
		}
		if (responseContentType == null) {
			logger.error(Log.RESPONSE, this.dump());
			throw new NoContentException(this);
		}
		// If we asked for XML and we got HTML, it must be an error page
		if ("text/html".equals(responseContentType))
			throw new InstanceUnavailableException(this);
		SAXBuilder parser = new SAXBuilder();
		Document responseDoc = null;	
		try {
			responseDoc = parser.build(new StringReader(responseText));
		} catch (JDOMException e) {
			logger.error(Log.RESPONSE, "REQUEST:\n" + requestText + "\nRESPONSE:\n" + responseText, e);
			throw new XmlParseException(uri, e);
		}
		return responseDoc;		
	}

}
