package sndml.datamart;

import java.util.Date;

import com.fasterxml.jackson.databind.JsonNode;

import sndml.servicenow.DateTime;
import sndml.servicenow.InvalidDateTimeException;

@Deprecated
public class DateExpression extends DateTime {

	public DateExpression(String value) throws InvalidDateTimeException {
		super(value);
	}

	public DateExpression(String value, int fmtlen) throws InvalidDateTimeException {
		super(value, fmtlen);
	}

//	@Deprecated
//	public DateTimeExpr(int year, int month, int day) {
//		super(year, month, day);
//	}

	public DateExpression(Date value) {
		super(value);
	}

//	public DateTimeExpr(Long seconds) {
//		super(seconds);
//	}
	
	public DateExpression(JsonNode node, DateCalculator factory) 
			throws ConfigParseException {
		super(factory.getDate(node.asText()));
	}

}
