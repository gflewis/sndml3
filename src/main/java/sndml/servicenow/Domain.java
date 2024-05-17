package sndml.servicenow;

/**
 * <p>Wrapper for a <b>sys_domain</b>.
 * This could be a single value or a comma separated list.</p>
 * <p>If Domain Separation is not in use then the domain will be null.</p> 
 * 
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
