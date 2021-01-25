package sndml.datamart;

@SuppressWarnings("serial")
public class ConfigParseException extends IllegalArgumentException {

	public ConfigParseException(String message) {
		super(message);
	}

	public ConfigParseException(Throwable cause) {
		super(cause);
	}

	public ConfigParseException(String message, Throwable cause) {
		super(message, cause);
	}


}
