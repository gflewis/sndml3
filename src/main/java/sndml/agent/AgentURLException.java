package sndml.agent;

import java.net.HttpURLConnection;
import java.net.URI;

@SuppressWarnings("serial")
public class AgentURLException extends AgentHandlerException {
	
	public AgentURLException(URI uri) {
		super(uri, HttpURLConnection.HTTP_BAD_REQUEST);
	}
	
	public AgentURLException(String path) {
		super(path, HttpURLConnection.HTTP_BAD_REQUEST);		
	}

}
