package servicenow.api;

import servicenow.datamart.Globals;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;

public class JsonFormatter {

	public static String format(String jsonText) throws JsonSyntaxException {
		if (Boolean.FALSE.equals(Globals.getBoolean("loader.debug_format_json"))) 
			return jsonText;
		JsonParser parser = new JsonParser();
		JsonObject obj = parser.parse(jsonText).getAsJsonObject();
		Gson gson = new GsonBuilder().setPrettyPrinting().create();	
		return gson.toJson(obj);			
	}
	
}
