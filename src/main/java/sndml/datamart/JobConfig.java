package sndml.datamart;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

import sndml.servicenow.*;

public class JobConfig {

	private LoaderConfig parent;
	private DateTimeFactory dateFactory;
	@JsonIgnore public DateTime start;
	public Key sys_id;
	public String number;
	public String name;
	public String source;
	public String target;
	public JobAction action;
	public Boolean truncate;
	@JsonProperty("drop") public Boolean dropTable;
	@JsonProperty("created") public JsonNode createdValue;
	@JsonProperty("since") public String sinceValue;
	@JsonIgnore public DateTimeRange createdRange;
	@JsonIgnore public DateTime sinceDate;
	public String filter;
	public DateTime.Interval partition;
	public Integer pageSize;
	public Integer minRows;
	public Integer maxRows;
	public String sqlBefore;
	public String sqlAfter;
	@JsonIgnore public FieldNames includeColumns;
	@JsonIgnore public FieldNames excludeColumns;
	public Integer threads;

	public JobConfig() {		
	}
	
	public void setDateFactory(DateTimeFactory factory) {
		this.dateFactory = factory;
	}

	/*
	
	public JobConfig(ObjectNode settings) {
		this.dateFactory = new DateTimeFactory();
		if (settings.has("number")) this.name = settings.get("number").asText();
		this.setPropertiesFrom(settings);
	}
	
	JobConfig(Table table) {
		this.name = table.getName();
		this.dateFactory = new DateTimeFactory();
	}

	JobConfig(LoaderConfig parent, JsonNode config) throws ConfigParseException {
		this.parent = parent;
		this.dateFactory = new DateTimeFactory(parent.getStart(), parent.getMetricsFile());
		if (config.isObject()) {
			setPropertiesFrom((ObjectNode) config);
		} else {
			name = config.asText();
		}
	}

	private void setPropertiesFrom(ObjectNode map) {
		Iterator<String> fieldnames = map.fieldNames();
		while (fieldnames.hasNext()) {
			String key = fieldnames.next();
			JsonNode val = map.get(key);
			switch (key.toLowerCase()) {
			case "name":
			case "number":
				this.name = val.asText();
				break;
			case "source":
				this.source = val.asText();
				break;
			case "target":
				this.target = val.asText();
				break;
			case "sys_id":
				this.sys_id = new Key(val.asText());
				break;
			case "action":
				switch (val.asText().toLowerCase()) {
				case "insert":
				case "load":
					this.action = JobAction.LOAD;
					break;
				case "update":
				case "refresh":
					this.action = JobAction.REFRESH;
					break;
				case "sync":
					this.action = JobAction.SYNC;
					break;
				case "prune":
					this.action = JobAction.PRUNE;
					break;
				case "create":
					this.action = JobAction.CREATE;
					break;
				default:
					throw new ConfigParseException("Not recognized: " + val.asText());
				}
				break;
			case "truncate":
				this.truncate = val.asBoolean();
				break;
			case "drop":
				this.dropTable = val.asBoolean();
				break;
			case "created":
				this.created = asDateRange(val);
				break;
			case "since":
				this.since = asDate(val);
				break;
			case "filter":
				this.filter = val.toString();
				break;
			case "partition":
				this.partition = asInterval(val);
				break;
			case "columns":
				this.includeColumns = new FieldNames(val.asText());
				break;
			case "exclude":
				this.excludeColumns = new FieldNames(val.asText());
				break;
			case "pagesize":
				this.pageSize = val.asInt();
				break;
			case "sqlbefore":
				this.sqlBefore = val.asText();
				break;
			case "sqlafter":
				this.sqlAfter = val.asText();
				break;
			case "minrows":
				if (!val.isInt()) configError("Not integer: minrows");
				this.minRows = val.asInt();
				break;
			case "maxrows":
				if (!val.isInt()) configError("Not integer: maxrows");
				this.maxRows = val.asInt();
				break;
			case "threads":
				this.threads = val.asInt();
				break;
			default:
				throw new ConfigParseException("Not recognized: " + key);
			}
		}		
	}
	*/

	@Deprecated
	void setDefaults(DateTimeFactory dateFactory) {
		if (start == null) {
			start = dateFactory.getStart();
		}
		if (action == null) {
			if (Boolean.TRUE.equals(truncate))
				action = JobAction.LOAD;
			else
				action = JobAction.REFRESH;
		}
		if (source == null) {
			source = (name != null ? name : target);
		}
		if (name == null) {
			name = (target != null ? target : source);
		}
		if (target == null) {
			target = (source != null ? source : name);
		}
		if (pageSize == null && parent != null) {
			pageSize = parent.getPageSize();
		}
	}
	
	public JobConfig validate() throws ConfigParseException {
		new JobConfigValidator(this).validate();
		return this;
	}

	void configError(String msg) {
		throw new ConfigParseException(msg);
	}

	@JsonIgnore
	void setCreated(DateTimeRange value) {
		this.createdRange = value;
	}
	
	void setCreated(JsonNode node) {
		assert node != null;
		assert dateFactory != null;
		if (node.isTextual()) {
			String s1 = node.asText();
			DateTime d1 = dateFactory.getDate(s1);
			this.createdRange = new DateTimeRange(d1, null);				
		}
		else if (node.isArray()) {
			int len = node.size();
			String s1 = len > 0 ? node.get(0).asText() : null;
			String s2 = len > 1 ? node.get(1).asText() : null;
			DateTime d1 = dateFactory.getDate(s1);
			DateTime d2 = dateFactory.getDate(s2);
			this.createdRange = new DateTimeRange(d1, d2);				
		}
		else
			throw new ConfigParseException("Invalid created: " + node);		
	}
	
	String getName() { return this.name; }
	String getSource() { return this.source; }
	String getTarget() { return this.target; }
	Key getId() { return this.sys_id; }
	JobAction getAction() { return action; }
	boolean getTruncate() {	return this.truncate == null ? false : this.truncate.booleanValue(); }
	boolean getDropTable() { return this.dropTable == null ? false : this.dropTable.booleanValue(); }	
	DateTime getSince() { return this.sinceDate; }
	String getFilter() { return this.filter; }
	
	// TODO: Is this best?
	DateTimeRange getCreated() { return (this.createdRange == null ? getDefaultRange() : this.createdRange); }	
	DateTime.Interval getPartitionInterval() { return this.partition; }
	
	FieldNames getIncludeColumns() { return this.includeColumns; }
	FieldNames getExcludeColumns() { return this.excludeColumns; }
	String getSqlBefore() {	return this.sqlBefore; }
	String getSqlAfter() { return this.sqlAfter; }
	Integer getPageSize() { return this.pageSize; }
	Integer getMinRows() { return this.minRows;	}
	Integer getMaxRows() { return this.maxRows;	}
	Integer getThreads() { return this.threads;	}
	
	/*
	void notValid(String option) {
		String msg = option + " not valid with Action: " + getAction().toString();
		configError(msg);
	}

	void optionsNotValid(FieldNames names) {
		for (String name : names) {
			if (settings.has(name.toLowerCase())) {
				notValid(name);
			}
		}		
	}
	
	void optionsNotValid(String names) {
		optionsNotValid(new FieldNames(names));
	}
	*/
	
	/*
	String getName() throws ConfigParseException {
		if (this.name != null)   return this.name;
		if (this.target != null) return this.target;
		if (this.source != null) return this.source;
		throw new ConfigParseException("Name not specified");
	}

	String getSource() throws ConfigParseException {
		if (this.source != null) return this.source;
		if (this.target != null) return this.target;
		if (this.name != null)   return this.name;
		throw new ConfigParseException("Source not specified");
	}

	String getTargetName() throws ConfigParseException {
		if (this.target != null) return this.target;
		if (this.source != null) return this.source;
		if (this.name != null)   return this.name;
		throw new ConfigParseException("Target not specified");
	}

		
	void setAction(JobAction action) {
		this.action = action;
	}

	JobAction getAction() {
		if (this.action == null)
			return this.getTruncate() ? JobAction.LOAD : JobAction.REFRESH;
		else
			return this.action;
	}

	void setTruncate(boolean truncate) {
		this.truncate = truncate;
	}
	*/
			

//	void setSince(DateTime since) {
//		this.since = since;
//	}


//	void setFilter(String value) {
//		this.filter = value;
//	}




	DateTimeRange getDefaultRange() {
		assert this.start != null;
		return new DateTimeRange(null, this.start);
	}

}
