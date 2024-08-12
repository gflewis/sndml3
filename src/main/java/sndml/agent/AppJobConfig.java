package sndml.agent;

import org.slf4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import sndml.loader.ConfigParseException;
import sndml.loader.JobConfig;
import sndml.servicenow.RecordKey;
import sndml.util.Log;

public class AppJobConfig extends JobConfig {

	// TODO Rename sys_id to runkey. sys_id is ambiguous.
	public RecordKey runKey;
	public String number;
	
	static final Logger logger = Log.getLogger(AppJobConfig.class);	
	
	public AppJobConfig() {
		super();
	}

	@Override
	synchronized protected void updateCoreFields() {
		super.updateCoreFields();
		if (jobName == null && number != null) jobName = number;		
	}
		
	@Override	
	synchronized public String toString() {
		ObjectMapper mapper = new ObjectMapper();
		ObjectNode node = mapper.createObjectNode();
		if (runKey != null) node.put("sys_id",  runKey.toString());
		if (number != null) node.put("number",  getNumber());
		addFieldsToObject(node);
		String yaml;
		try {
			yaml = mapper.writeValueAsString(node);
		} catch (JsonProcessingException e) {
			throw new ConfigParseException(e);
		}
		return yaml;
	}

	public RecordKey getRunKey() {
		assert this.runKey != null;
		return this.runKey; 
	}	
	
	public String getNumber() {
		assert this.number != null;
		return this.number; 
	}
	
	@Override
	public String getName() { 
		return getNumber(); 
	}
	
}
