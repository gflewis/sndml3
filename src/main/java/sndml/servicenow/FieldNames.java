package sndml.servicenow;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;

@SuppressWarnings("serial")
public class FieldNames extends ArrayList<String> {

	public FieldNames() {
		super();
	}

	public FieldNames(int size) {
		super(size);
	}
	
	public FieldNames(String str) {
		super();
		addAll(Arrays.asList(str.split("[,\\s]+")));
	}
	
	public FieldNames(Set<String> names) {
		super(names);
	}
	
	public FieldNames addKey() {
		if (!this.contains("sys_id")) {
			this.add(0, "sys_id");
		}
		return this;
	}
		
	public String toString() {
		StringBuffer result = new StringBuffer();
		String delim = "";
		for (String name : this) {
			result.append(delim).append(name);
			delim = ",";
		}
		return result.toString();
	}
	
	@Override
	public String[] toArray() {
		String[] result = new String[this.size()];
		result = this.toArray(result);
		return result;
	}
}
