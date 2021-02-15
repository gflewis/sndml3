package sndml.datamart;

/**
 * Contains the JDBC/SQL type and the Glide type for a single field.
 */
public class DatabaseFieldDefinition {

	final String name;
	final int sqltype;
	final int size;
	final String glidename;
	
	DatabaseFieldDefinition(String name, int sqltype, int size, String glidename) {
		this.name = name;
		this.sqltype = sqltype;
		this.size = size;	
		this.glidename = glidename;
	}
	
	public String getName() { return this.name; }
	public int getType() { return this.sqltype; }
	public int getSize() { return this.size; }
	public String getGlideName() { return this.glidename; }
	
}
