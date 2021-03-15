package sndml.datamart;

import java.util.EnumSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import sndml.servicenow.*;

public class JobConfig {

	static final Logger logger = LoggerFactory.getLogger(JobConfig.class);	
	
	public DateTime start;
	public DateTime last;
	public Key sys_id;
	public String number;
	@JsonProperty("name") public String jobName;
	public String source;
	public String target;
	public Action action;
	public Boolean truncate;
	@JsonProperty("drop") public Boolean dropTable;
	@JsonProperty("created") public JsonNode createdExpr;
	@JsonProperty("since") public String sinceExpr;
	@JsonIgnore public DateTimeRange createdRange;
	@JsonIgnore public DateTime sinceDate;
	public String filter;
	public Interval partition;
	public Integer pageSize;
	public Integer minRows;
	public Integer maxRows;
	public String sql; // Action EXECUTE only
	public String sqlBefore;
	public String sqlAfter;
	public Boolean autoCreate;
	@JsonIgnore public FieldNames includeColumns;
	public Integer threads;
	
	static EnumSet<Action> anyLoadAction =
			EnumSet.of(Action.INSERT, Action.UPDATE, Action.SYNC);
	
	String getName() {
		assert jobName != null;
		return jobName; 
	}
	
	String getSource() {
		assert source != null;
		return source; 		
	}
	
	String getTarget() {
		assert target != null;
		return this.target; 
	}
	
	Key getSysId() { return this.sys_id; }
	String getNumber() { return this.number; }
	Action getAction() { return action; }
	boolean getTruncate() {	return this.truncate == null ? false : this.truncate.booleanValue(); }
	boolean getDropTable() { return this.dropTable == null ? false : this.dropTable.booleanValue(); }
	DateTime getSince() { return this.sinceDate; }
	DateTimeRange getCreated() { return this.createdRange; }
	String getFilter() { return this.filter; }
		
	Interval getPartitionInterval() { return this.partition; }
	
	FieldNames getIncludeColumns() { return this.includeColumns; }
	
	String getSql() { return this.sql; }
	String getSqlBefore() {	return this.sqlBefore; }
	String getSqlAfter() { return this.sqlAfter; }
	Integer getPageSize() { return this.pageSize; }
	Integer getMinRows() { return this.minRows;	}
	Integer getMaxRows() { return this.maxRows;	}
	Integer getThreads() { return this.threads;	}	

	boolean getAutoCreate() { 
		return this.autoCreate == null ? true : this.autoCreate.booleanValue();	
	}
	
	void setColumns(String columnNames) {
		includeColumns = new FieldNames();
		includeColumns.add("sys_id");
		includeColumns.add("sys_created_on");
		includeColumns.add("sys_updated_on");
		FieldNames temp = new FieldNames(columnNames);
		for (String name : temp) {
			if (!includeColumns.contains(name)) includeColumns.add(name);
		}
	}
	
	FieldNames getColumns() {
		return this.includeColumns;
	}
			
	DateTimeRange getDefaultRange() {
		assert this.start != null;
		return new DateTimeRange(null, this.start);
	}

	void initializeAndValidate(ConnectionProfile profile, DateCalculator dateCalculator) {
		initialize(profile, dateCalculator);
		validate();		
	}
	
	void initialize(ConnectionProfile profile, DateCalculator dateCalculator) {
		updateCoreFields();
		updateDateFields(dateCalculator);
		if (profile != null) updateFromProfile(profile);
	}

	private void updateCoreFields() {
		// Determine Action
		if (action == null)	action = 
				Boolean.TRUE.equals(truncate) ?	
				Action.INSERT : Action.UPDATE;
		if (action == Action.LOAD)    action = Action.INSERT;
		if (action == Action.REFRESH) action = Action.UPDATE;
		
		// Determine Source, Target and Name
		if (source == null)  source = (jobName != null) ? jobName : target;
		if (jobName == null) jobName = (target != null) ? target : source;
		if (target == null)	 target = (source != null) ? source : jobName;		
	}
	
	private void updateDateFields(DateCalculator calculator) {
		if (calculator == null) calculator = new DateCalculator();
		if (start != null) calculator.setStart(start);
		if (last != null) calculator.setLast(last);
		if (sinceExpr == null) {
			sinceDate = null;
		}
		else {
			sinceDate = calculator.getDate(sinceExpr);
		}
		
		if (createdExpr == null) {
			createdRange = null;
		}
		if (createdExpr != null) {
			setCreated(createdExpr, calculator);
		}			
	}

	@JsonIgnore
	void setCreated(DateTimeRange value) {
		this.createdRange = value;
	}
		
	private void setCreated(JsonNode node, DateCalculator calculator) {
		assert node != null;
		assert calculator != null;
		DateTime defaultEnd = calculator.getStart();
		assert defaultEnd != null;
		if (node.isTextual()) {
			String s1 = node.asText();
			DateTime d1 = calculator.getDate(s1);
			this.createdRange = new DateTimeRange(d1, defaultEnd);				
		}
		else if (node.isArray()) {
			int len = node.size();
			String s1 = len > 0 ? node.get(0).asText() : null;
			String s2 = len > 1 ? node.get(1).asText() : null;
			DateTime d1 = s1 == null ? null : calculator.getDate(s1);
			DateTime d2 = s2 == null ? defaultEnd : calculator.getDate(s2);
			this.createdRange = new DateTimeRange(d1, d2);				
		}
		else
			throw new ConfigParseException("Invalid created: " + node);		
	}
		
	void updateFromProfile(ConnectionProfile profile) {		
		// AutoCreate defaults to True
		if (autoCreate == null) {
			if (anyLoadAction.contains(action))
				autoCreate = profile.getPropertyBoolean("datamart.autocreate", true);
			else
				autoCreate = false;
		}				
	}
		
	void validate() {
		if (action == Action.EXECUTE) {
			if (sql == null) configError("Missing SQL");
		}
		else {
			if (action == null) configError("Action not specified");
			if (source == null) configError("Source not specified");
			if (target == null) configError("Target not specified");
			if (jobName == null) configError("Name not specified");
		}
		booleanValidForActions("Truncate", truncate, EnumSet.of(Action.INSERT));
		booleanValidForActions("Drop", dropTable, EnumSet.of(Action.CREATE));
		validForActions("Created", createdRange, Action.INSERT_UPDATE_SYNC);
		validForActions("Partition", partition, Action.INSERT_UPDATE_SYNC);
		validForActions("Filter", filter, Action.INSERT_UPDATE);
		validForActions("Since", sinceDate, Action.INSERT_UPDATE_PRUNE);
		validForActions("SQL", sql, Action.EXECUTE_ONLY);
		
		if (sinceDate == null && sinceExpr != null)
			configError("Missing Since Date");
		if (createdRange == null & createdExpr != null)
			configError("Missing Created Range");
		
//		if (getIncludeColumns() != null && getExcludeColumns() != null) 
//			configError("Cannot specify both Columns and Exclude");
		if (threads != null && partition == null)
			configError("Threads only valid with Partition");
		
		if (sqlBefore != null) logger.warn(Log.INIT, "Deprecated option: SQLBefore");
		if (sqlAfter != null) logger.warn(Log.INIT, "Deprecated option: SQLAfter");		
	}
		
	void booleanValidForActions(String name, Boolean value, EnumSet<Action> validActions) {
		if (Boolean.TRUE.equals(value)) {
			if (!validActions.contains(action))
				notValid(name, action);
		}
	}
	
	void validForActions(String name, Object value, EnumSet<Action> validActions)
			throws ConfigParseException {
		if (value != null) {
			if (!validActions.contains(action))
				notValid(name, action);
		}		
	}
		
	static void notValid(String option, Action action) throws ConfigParseException {
		String msg = option + " not valid with Action: " + action.toString();
		configError(msg);
	}

	static void configError(String msg) {
		throw new ConfigParseException(msg);
	}	
		
	public String toString() {
		ObjectMapper mapper = new ObjectMapper();
		ObjectNode node = mapper.createObjectNode();
		if (sys_id != null) node.put("sys_id",  sys_id.toString());
		if (number != null) node.put("number",  getNumber());
		node.put("name", getName());
		node.put("source", getSource());
		node.put("target", getTarget());
		node.put("action", action.toString());
		if (getTruncate()) node.put("truncate", true);
		if (getDropTable()) node.put("drop", true);
		if (getAutoCreate()) node.put("autocreate", getAutoCreate());
		if (getSince() != null) node.put("since", getSince().toString());
		if (getCreated() != null) node.set("created", getCreated().toJsonNode());
		if (getPartitionInterval() != null) node.put("partition",  getPartitionInterval().toString());
		if (getFilter() != null) node.put("filter", getFilter());
		if (includeColumns != null) node.put("columns", includeColumns.toString());
		String yaml;
		try {
			yaml = mapper.writeValueAsString(node);
		} catch (JsonProcessingException e) {
			throw new ConfigParseException(e);
		}
		return yaml;
	}
		
}
