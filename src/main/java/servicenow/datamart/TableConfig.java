package servicenow.datamart;

import servicenow.api.*;

public class TableConfig extends Config {

	private Map items;
	
	private String name;
	private String source;
	private String target;
	private LoaderAction action;
	private Boolean truncate;
	private DateTimeRange created;
	private DateTimeRange updated;
	private DateTime since;
	private EncodedQuery filter;
	private DateTime.Interval partition;
	private Integer pageSize = null;
	private Integer threads = null;
	private DateTimeFactory dateFactory;

	public TableConfig(Table table) {
		this.name = table.getName();
	}
	
	public TableConfig(LoaderConfig parent, Object config) throws ConfigParseException {
		if (isMap(config)) {
			assert parent != null;
			dateFactory = new DateTimeFactory();
			items = new Config.Map(config);
			for (String key : items.keySet()) {
			    Object val = items.get(key);
			    switch (key.toLowerCase()) {
			    case "name":
			    		this.name = val.toString();
			    		break;
			    case "source":
			    		this.source = val.toString();
			    		break;
			    case "target": 
			    		this.target = val.toString(); 
			    		break;
			    case "action":
			    		switch (val.toString().toLowerCase()) {
			    		case "update": this.action = LoaderAction.UPDATE; break;
			    		case "insert": this.action = LoaderAction.INSERT; break;
			    		case "prune":  this.action = LoaderAction.PRUNE; break;
			    		default:
						throw new ConfigParseException("Not recognized: " + val.toString());			    			
			    		}
			    		break;
			    case "truncate":
			    		this.truncate = (Boolean) val;
			    		break;
			    case "created":
			    		this.created = asDateRange(val);
			    		break;
//			    case "updated":
//			    		this.updated = asDateRange(val);
//			    		break;
			    case "since":
			    		this.since = asDate(val);
			    		break;
			    case "filter":
			    		this.filter = new EncodedQuery(val.toString());
			    		break;
			    case "partition":
			    		this.partition = asInterval(val);
			    		break;
			    case "pagesize" :
			    		this.pageSize = asInteger(val);
			    		break;
			    case "threads" :
			    		this.threads = asInteger(val);
			    		break;
			    	default:
			    		throw new ConfigParseException("Not recognized: " + key);
			    }
			}
		}
		else {
			if (config instanceof String)
				name = (String) config;
			else
				throw new ConfigParseException("Not recognized: " + config.toString());
		}		
	}
	
	public void validate() throws ConfigParseException {
		
	}
			
	public String getName() throws ConfigParseException {
		if (this.name != null) return this.name;
		if (this.target != null) return this.target;
		if (this.source != null) return this.source;
		throw new ConfigParseException("Name not specified");
	}

	public String getSource() throws ConfigParseException {
		if (this.source != null) return this.source;
		if (this.target != null) return this.target;
		if (this.name != null) return this.name;
		throw new ConfigParseException("Source not specified");
	}
	
	public String getTargetName() throws ConfigParseException {
		if (this.target != null) return this.target;
		if (this.source != null) return this.source;
		if (this.name != null) return this.name;
		throw new ConfigParseException("Target not specified");
	}

	public LoaderAction getAction() {
		if (this.action == null) 
			return this.getTruncate() ? LoaderAction.INSERT : LoaderAction.UPDATE;
		else
			return this.action;
	}
	
	public boolean getTruncate() {
		return this.truncate == null ? false : this.truncate.booleanValue();
	}
	
	public DateTimeRange getCreated() {
		return this.created;
	}
	
	@Deprecated
	public DateTimeRange getUpdated() {
		return this.updated;
	}
	
	public DateTime getSince() {
		return this.since;
	}
	
	public EncodedQuery getFilter() {
		return this.filter;
	}
	
	public DateTime.Interval getPartitionInterval() {
		return this.partition;
	}
	
	public Integer getPageSize() {
		return this.pageSize;
	}

	public Integer getThreads() {
		return this.threads;
	}
	
	public DateTime.Interval asInterval(Object obj) throws ConfigParseException {
		DateTime.Interval result;
		try {
			result = DateTime.Interval.valueOf(obj.toString().toUpperCase());
		}
		catch (IllegalArgumentException e) {
			throw new ConfigParseException("Invalid partition: " + obj.toString());
		}
		return result;
	}
	
	public DateTime asDate(Object obj) {
		return dateFactory.getDate(obj);
	}
	
	public DateTimeRange asDateRange(Object obj) throws ConfigParseException {
		DateTime start, end;
		end = dateFactory.getStart();
		if (isList(obj)) {
			List dates = new Config.List(obj);
			if (dates.size() < 1 || dates.size() > 2) 
				throw new ConfigParseException("Invalid date range: " + obj.toString());
			start = dateFactory.getDate(dates.get(0));
			if (dates.size() > 1) end = dateFactory.getDate(dates.get(1));
		}
		else {
			start = dateFactory.getDate(obj);
		}
		return new DateTimeRange(start, end);
	}
	
}
