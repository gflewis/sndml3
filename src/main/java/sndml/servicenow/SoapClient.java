package sndml.servicenow;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.Namespace;
import org.slf4j.Logger;

public class SoapClient {

	final Session session;
	final String tablename;
	final String uriPath;
	
	final Namespace nsTNS;
	final Logger logger = Log.logger(this.getClass());
	
	final static Namespace nsSoapEnv = 
		Namespace.getNamespace("env", "http://schemas.xmlsoap.org/soap/envelope/");
    final static Namespace nsSoapEnc = 
    		Namespace.getNamespace("enc", "http://schemas.xmlsoap.org/soap/encoding/");
	
	public SoapClient(Table table) throws IOException {
		this(table.getSession(), table.getName());
	}
	
	public SoapClient(Session session, String tablename) {
		this(session, tablename, "SOAP");
	}
	
	public SoapClient(Session session, String tablename, String protocol) {
		assert session != null;
		this.session = session;
		this.tablename = tablename;
		this.nsTNS = Namespace.getNamespace("tns", "http://www.service-now.com/" + tablename);
		this.uriPath = tablename + ".do?" + protocol;
	}

	public Session getSession() {
		return this.session;
	}
		
	public Element executeRequest(String methodName, Parameters docParams) throws IOException {
		return executeRequest(methodName, docParams, null, null);
	}

	public Element executeRequest(
			String methodName, 
			Parameters docParams, 
			Parameters uriParams, 
			String responseElementName) throws IOException {
		
		URI uri = session.getURI(this.uriPath, uriParams);
//		Log.setSessionContext(session);
//		Log.setTableContext(tablename);
//		Log.setMethodContext(methodName);
//		Log.setURIContext(uri);
		Element method = createXmlElement(methodName, docParams);
		Document requestDoc = createSoapDocument(method);	
		if (logger.isDebugEnabled()) {
			String requestText = XmlFormatter.format(requestDoc);
			logger.debug(Log.REQUEST, "\n" + requestText);
		}
		
		XmlRequest xmlRequest = new XmlRequest(session.getClient(), uri, requestDoc);
		Document responseDoc = xmlRequest.getDocument();
		if (logger.isDebugEnabled()) {
			String responseText = XmlFormatter.format(responseDoc);
			logger.debug(Log.RESPONSE, "\n" + Log.abbreviate(responseText));
			// logger.trace(Log.RESPONSE, "\n" + responseText);
		}
		
		Element responseBody = responseDoc.getRootElement().getChild("Body", nsSoapEnv);
		Element responseElement = responseBody.getChildren().get(0);
		if (responseElement.getName().equals("Fault")) { 
			String faultString = getFaultString(responseElement);
			assert faultString != null;
			logger.error(Log.RESPONSE, faultString);
			if (StringUtils.containsIgnoreCase(faultString, "insufficient rights"))
				throw new InsufficientRightsException(xmlRequest);
			if (StringUtils.containsIgnoreCase(faultString,  "missing record"))
				throw new NoSuchRecordException(faultString);
			throw new SoapResponseException(tablename, faultString);			
		}
		if (responseElementName != null && !responseElementName.equals(responseElement.getName()))
			throw new ServiceNowError(
					"responseElementName expected=" + responseElementName + "; found=" + responseElement.getName());
		return responseElement;		
	}
	
	Document createSoapDocument(Element content) {
	    Element requestHeader = new Element("Header", nsSoapEnv);
	    Element requestBody = new Element("Body", nsSoapEnv);
	    requestBody.addContent(content);    
	    Element envelope = new Element("Envelope", nsSoapEnv);
		envelope.addNamespaceDeclaration(nsSoapEnv);
		envelope.addNamespaceDeclaration(nsSoapEnc);
		envelope.addNamespaceDeclaration(content.getNamespace());
		envelope.addContent(requestHeader);
		envelope.addContent(requestBody);
		return new Document(envelope);
	}
	
	static String getFaultString(Element responseElement) {
		if (responseElement.getName().equals("Fault")) 
			return responseElement.getChildText("faultstring");
		else
			return null;		
	}
	
	private Element createXmlElement(String methodName, Parameters params) {
		Element element = new Element(methodName, nsTNS);
		if (params != null) {
			Set<String> keys = params.keySet();
			Iterator<String> iter = keys.iterator();
			while (iter.hasNext()) {
				String name = iter.next();
				String value = params.get(name);
				element.addContent(createXmlText(name, value));
			}			
		}
		return element;
	}

	private Element createXmlText(String name, String value) {
		Element ele = new Element(name);
		ele.setText(value);
		return ele;
	}
	
}
