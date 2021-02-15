package sndml.servicenow;


@SuppressWarnings("serial")
public class InvalidDateTimeException extends IllegalArgumentException {

	InvalidDateTimeException(String value) {
		super(value);
	}
	
	InvalidDateTimeException(String tablename, String fieldname, String value) {
		super(tablename + "." + fieldname + "=" + value);
	}
	
}
