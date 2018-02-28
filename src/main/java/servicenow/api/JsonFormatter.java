package servicenow.api;

//import java.io.IOException;
//import com.fasterxml.jackson.databind.JsonNode;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;

public class JsonFormatter {

	/*
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
	*/
	
	
	public static String format(String jsonText) throws JsonSyntaxException {
		JsonParser parser = new JsonParser();
		JsonObject obj = parser.parse(jsonText).getAsJsonObject();
		Gson gson = new GsonBuilder().setPrettyPrinting().create();	
		return gson.toJson(obj);
	}
	
}
