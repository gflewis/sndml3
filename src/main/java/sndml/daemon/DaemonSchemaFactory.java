package sndml.daemon;

import java.io.IOException;
import java.net.URI;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import sndml.servicenow.*;

public class DaemonSchemaFactory {

	private final Session session;
	
	public DaemonSchemaFactory(Session session) {
		this.session = session;
	}
	
	TableSchema getSchema(String tablename) throws IOException {
		assert tablename != null;
		Table table = session.table(tablename);
		TableSchema schema = new TableSchema(table);
		URI apiTableSchema = Daemon.getAPI(session, "gettableschema", tablename);
		JsonRequest request = new JsonRequest(session, apiTableSchema);
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
