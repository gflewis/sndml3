package sndml.servicenow;

/**
 * Wrapper for a <b>sys_domain</b>.
 * This could be a single value or a comma separated list.  
 */
public class Domain {
	
	final String value;
	
	public Domain(String value) {
		this.value = value;
	}	
	
	public String toString() {
		return value;
	}

}
