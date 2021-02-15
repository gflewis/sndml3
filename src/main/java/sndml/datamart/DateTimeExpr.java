package sndml.datamart;

import java.util.Date;

import com.fasterxml.jackson.databind.JsonNode;

import sndml.servicenow.DateTime;
import sndml.servicenow.InvalidDateTimeException;

@Deprecated
public class DateTimeExpr extends DateTime {

	public DateTimeExpr(String value) throws InvalidDateTimeException {
		super(value);
	}

	public DateTimeExpr(String value, int fmtlen) throws InvalidDateTimeException {
		super(value, fmtlen);
	}

//	@Deprecated
//	public DateTimeExpr(int year, int month, int day) {
//		super(year, month, day);
//	}

	public DateTimeExpr(Date value) {
		super(value);
	}

//	public DateTimeExpr(Long seconds) {
//		super(seconds);
//	}
	
	public DateTimeExpr(JsonNode node, DateTimeFactory factory) 
			throws ConfigParseException {
		super(factory.getDate(node.asText()));
	}

}
