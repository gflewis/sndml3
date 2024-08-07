package sndml.agent;

import java.io.IOException;
import java.io.InputStream;

import com.fasterxml.jackson.core.exc.StreamReadException;
import com.fasterxml.jackson.databind.DatabindException;
import com.fasterxml.jackson.databind.ObjectMapper;

@Deprecated
public class JobActionRequest {

	enum JobActionType {
		START,
		CANCEL
	};
	
	public String instance;
	public String agent;
	public String sys_id;
	public JobActionType action;
	
	static final ObjectMapper mapper = new ObjectMapper();
		
	static public JobActionRequest load(InputStream input) 
			throws StreamReadException, DatabindException, IOException {
		return mapper.readValue(input, JobActionRequest.class);		
	}

}
