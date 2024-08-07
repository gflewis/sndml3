package sndml.agent;

import java.net.URI;

@SuppressWarnings("serial")
public class AgentHandlerException extends Exception {

	int returnCode;
	
	public AgentHandlerException(Throwable cause, int returnCode) {
		super(cause);
		this.returnCode = returnCode;
	}

	public AgentHandlerException(String path, int returnCode) {
		super("Path=" + path);
		this.returnCode = returnCode;
	}

	public AgentHandlerException(URI uri, int returnCode) {
		super("URI=" + uri.toString());
		this.returnCode = returnCode;
	}
	
	int getReturnCode() {
		return this.returnCode;
	}
		
}
