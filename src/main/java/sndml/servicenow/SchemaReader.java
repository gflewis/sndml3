package sndml.servicenow;

import java.io.IOException;

public interface SchemaReader {

	TableSchema getSchema(String tablename) throws IOException, InterruptedException;

}
