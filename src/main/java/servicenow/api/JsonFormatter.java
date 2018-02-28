package servicenow.api;

import java.io.IOException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

public class JsonFormatter {

	static ObjectMapper mapper;
	static ObjectWriter prettyWriter;
	
	static {
		mapper = new ObjectMapper();
		prettyWriter = mapper.writerWithDefaultPrettyPrinter();		
	}
	
	public static String format(String jsonText) throws IOException {
		JsonNode node = mapper.readTree(jsonText);
		String result = prettyWriter.writeValueAsString(node);
		return result;		
	}
	
}
