package sndml.servicenow;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Properties;

import org.apache.http.HttpHost;
import org.apache.http.client.utils.URIBuilder;

/**
 * Holds the URL for a ServiceNow instance.
 * Does not hold connection credentials, cookies or session ID.
 */
public class Instance {

	final HttpHost host;
	final URL url;

	public Instance(URL url) {
		this.url = url;
		this.host = new HttpHost(url.getHost());
	}

	public Instance(Properties properties) {
		this(properties.getProperty("servicenow.instance"));
	}
	
	public Instance(String name) {
		assert name != null;
		assert name.length() > 0;
		try {
			this.url = getURL(name);
		} catch (MalformedURLException e) {
			throw new ServiceNowError(e);
		}
		this.host = new HttpHost(url.getHost());
	}
	
	private URL getURL(String name) throws MalformedURLException {
		if (name == null || name.length() == 0)
			throw new AssertionError("Instance URL or name not provided");
		if (name.matches("[\\w-]+")) {
			// name is the instance name; build the URL
			return new URL("https://" + name + ".service-now.com/");			
		}
		if (name.startsWith("https://")) {
			// name is the the full URL
			// make sure it ends with a slash
			if (!name.endsWith("/")) name += "/";
			return new URL(name);			
		}
		throw new AssertionError("Instance URL not valid: " + name);
	}
	
	public URI getURI(String path) {
		return getURI(path, null);
	}
		
	public URI getURI(String path, Parameters params) {
		assert path != null;
		assert path.length() > 0;
		URI result;
		try {
			String base = url.toString() + path;
			URIBuilder builder = new URIBuilder(base);
			if (params != null) builder.addParameters(params.nvpList());
			result = builder.build();			
		}
		catch (URISyntaxException e) {
			throw new ServiceNowError(e);
		}
		return result;
	}
		
	public URL getURL() {
		return this.url;
	}
	
	public HttpHost getHost() {
		return this.host;
	}

	public String toString() {
		return url.toString();
	}
	
}
