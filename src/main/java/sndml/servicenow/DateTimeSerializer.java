package sndml.servicenow;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.*;


@SuppressWarnings("serial")
public class DateTimeSerializer extends StdSerializer<DateTime> {

	public DateTimeSerializer() {
		this(null);
	}
	
	public DateTimeSerializer(Class<DateTime> t) {
		super(t);
	}

	@Override
	public void serialize(DateTime value, JsonGenerator gen, SerializerProvider provider) 
			throws IOException {
		gen.writeString(value.toString());		
	}

}
