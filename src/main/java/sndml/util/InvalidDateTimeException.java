package sndml.util;


@SuppressWarnings("serial")
public class InvalidDateTimeException extends IllegalArgumentException {

	InvalidDateTimeException(String value) {
		super(String.format("Invalid date time \"%s\"", value));
	}
	
	InvalidDateTimeException(String tablename, String fieldname, String value) {
		super(tablename + "." + fieldname + "=" + value);
	}
	
}
