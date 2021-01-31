package sndml.datamart;

import java.util.Iterator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import sndml.servicenow.*;

public class JobConfig {

	private LoaderConfig parent;
	private Key id;
	private String number;
	private String name;
	private String source;
	private String target;
	private LoaderAction action;
	private Boolean truncate;
	private DateTimeRange created;
	private DateTime since;
	private String filter;
	private DateTime.Interval partition;
	private Integer pageSize;
	private Integer minRows;
	private Integer maxRows;
	private String sqlBefore;
	private String sqlAfter;
	private FieldNames includeColumns;
	private FieldNames excludeColumns;
	private Integer threads;
	private final DateTimeFactory dateFactory;

	public JobConfig(ObjectNode map) {
		this.dateFactory = new DateTimeFactory();
		this.name = map.get("number").asText();
		this.setPropertiesFrom(map);;
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
				this.name = val.asText();
				break;
			case "source":
				this.source = val.asText();
				break;
			case "target":
				this.target = val.asText();
				break;
			case "id":
				this.id = new Key(val.asText());
				break;
			case "number":
				this.number = val.asText();
				break;
			case "action":
				switch (val.asText().toLowerCase()) {
				case "update":
				case "refresh":
					this.action = LoaderAction.UPDATE;
					break;
				case "insert":
				case "load":
					this.action = LoaderAction.INSERT;
					break;
				case "prune":
					this.action = LoaderAction.PRUNE;
					break;
				case "sync":
					this.action = LoaderAction.SYNC;
					break;
				case "droptable":
					this.action = LoaderAction.DROPTABLE;
					break;
				default:
					throw new ConfigParseException("Not recognized: " + val.asText());
				}
				break;
			case "truncate":
				this.truncate = val.asBoolean();
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
				this.minRows = val.asInt();
				break;
			case "maxrows":
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
	
	public JobConfig validate() throws ConfigParseException {
		if (name == null && source == null && target == null)
			configError("Must specify at least one of Name, Source, Target");
		switch (getAction()) {
		case UPDATE:
			break;
		case INSERT:
			break;
		case PRUNE:
			if (getTruncate())     notValid("Truncate");
			if (created != null)   notValid("Created");
			if (filter != null)    notValid("Filter");
			if (threads != null)   notValid("Threads");
			if (partition != null) notValid("Partition");
			break;
		case SYNC:
			if (getTruncate())     notValid("Truncate");
			if (since != null)     notValid("Since");
			if (filter != null)	   notValid("Filter");
			break;
		case DROPTABLE:
			if (getTruncate())     notValid("Truncate");
			if (created != null)   notValid("Created");
			if (filter != null)    notValid("Filter");
			if (threads != null)   notValid("Threads");
			if (partition != null) notValid("Partition");
			break;
		}
		if (includeColumns != null && excludeColumns != null) 
			configError("Cannot specify both Columns and Exclude");
		return this;
	}

	void notValid(String option) {
		String msg = option + " not valid with Action: " + getAction().toString();
		configError(msg);
	}

	void configError(String msg) {
		throw new ConfigParseException(msg);
	}

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

	Key getId() {
		return this.id;
	}
	
	String getNumber() {
		return this.number;
	}
	
	void setAction(LoaderAction action) {
		this.action = action;
	}

	LoaderAction getAction() {
		if (this.action == null)
			return this.getTruncate() ? LoaderAction.INSERT : LoaderAction.UPDATE;
		else
			return this.action;
	}

	void setTruncate(boolean truncate) {
		this.truncate = truncate;
	}
	
	boolean getTruncate() {
		return this.truncate == null ? false : this.truncate.booleanValue();
	}
	
	void setCreated(DateTimeRange value) {
		this.created = value;
	}

	DateTimeRange getCreated() {
		if (this.created == null)
			return getDefaultRange();
		else
			return this.created;
	}

	void setSince(DateTime since) {
		this.since = since;
	}

	DateTime getSince() {
		return this.since;
	}

	void setFilter(String value) {
		this.filter = value;
	}

	String getFilter() {
		return this.filter;
	}

	FieldNames getIncludeColumns() {
		return this.includeColumns;
	}
	
	FieldNames getExcludeColumns() {
		return this.excludeColumns;
	}

	String getSqlBefore() {
		return this.sqlBefore;
	}

	String getSqlAfter() {
		return this.sqlAfter;
	}

	Integer getPageSize() {
		if (pageSize != null)
			return pageSize;
		if (parent != null)
			return parent.getPageSize();
		return null;
	}

	Integer getMinRows() {
		return this.minRows;
	}

	Integer getMaxRows() {
		return this.maxRows;
	}

	Integer getThreads() {
		return this.threads;
	}

	DateTime.Interval getPartitionInterval() {
		return this.partition;
	}

	DateTime.Interval asInterval(Object obj) throws ConfigParseException {
		DateTime.Interval result;
		try {
			result = DateTime.Interval.valueOf(obj.toString().toUpperCase());
		} catch (IllegalArgumentException e) {
			throw new ConfigParseException("Invalid partition: " + obj.toString());
		}
		return result;
	}

	DateTime asDate(JsonNode obj) {
		return dateFactory.getDate(obj);
	}

	DateTimeRange asDateRange(JsonNode obj) {
		DateTime start, end;
		end = dateFactory.getStart();
		if (obj.isArray()) {
			ArrayNode dates = (ArrayNode) obj;
			if (dates.size() < 1 || dates.size() > 2)
				throw new ConfigParseException("Invalid date range: " + obj.toString());
			start = dateFactory.getDate(dates.get(0));
			if (dates.size() > 1)
				end = dateFactory.getDate(dates.get(1));
		} else {
			start = dateFactory.getDate(obj);
		}
		return new DateTimeRange(start, end);
	}

	DateTimeRange getDefaultRange() {
		assert dateFactory != null;
		return new DateTimeRange(null, dateFactory.getStart());
	}

}
