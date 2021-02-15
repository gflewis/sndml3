package sndml.servicenow;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

@SuppressWarnings("serial")
@Deprecated
public class DateTimeDeserializer extends StdDeserializer<DateTime> {

	public DateTimeDeserializer(Class<?> vc) {
		super(vc);
		// TODO Auto-generated constructor stub
	}

	public DateTimeDeserializer(JavaType valueType) {
		super(valueType);
		// TODO Auto-generated constructor stub
	}

	public DateTimeDeserializer(StdDeserializer<?> src) {
		super(src);
		// TODO Auto-generated constructor stub
	}

	@Override
	public DateTime deserialize(JsonParser p, DeserializationContext ctxt) 
			throws IOException, JsonProcessingException {
		// TODO Auto-generated method stub
		return null;
	}

}
