package sndml.agent;

import java.io.IOException;
import java.net.URI;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

//import sndml.loader.ConnectionProfile;
import sndml.servicenow.*;

public class AppSchemaReader implements SchemaReader {

	private final AppSession appSession;
//	private final ConnectionProfile profile;
	
	public AppSchemaReader(AppSession session) {
//		this.profile = AgentDaemon.getConnectionProfile();
		this.appSession = session;
	}
	
	@Override
	public TableSchema getSchema(String tablename) throws IOException {
		assert tablename != null;
		Table table = appSession.table(tablename);
		TableSchema schema = new TableSchema(table);
		URI apiTableSchema = appSession.getAPI("gettableschema", tablename);
		JsonRequest request = new JsonRequest(appSession, apiTableSchema);
		ObjectNode response = request.execute();
		JsonNode result = response.get("result");
		assert result.isArray();
		ArrayNode elements = (ArrayNode) result;
		for (JsonNode element : elements) {
			String name = element.get("name").asText();
			String type = element.get("type").asText();
			int len = element.get("len").asInt();
			String ref = element.get("ref").asText();
			assert name.length() > 0;
			assert type.length() > 0;
			assert len > 0;
			schema.addField(name, type, len, ref);
		}
		assert schema.numFields() > 0;
		return schema;
	}

}
