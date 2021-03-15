package sndml.datamart;

import sndml.servicenow.DateTimeRange;
import sndml.servicenow.EncodedQuery;
import sndml.servicenow.FieldNames;
import sndml.servicenow.Table;
import sndml.servicenow.TableReader;
import sndml.servicenow.TableReaderFactory;

public abstract class ConfigTableReaderFactory extends TableReaderFactory {

	final JobConfig config;
	
	public ConfigTableReaderFactory(Table table, JobConfig config) {
		super(table);
		this.config = config;
	}

	@Override
	public TableReader createReader() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public EncodedQuery getFilter() { 
		return new EncodedQuery(table, config.getFilter()); 
	}
	
	@Override
	public DateTimeRange getCreatedRange() {
		return config.getCreated();
	}
	
	@Override
	public DateTimeRange getUpdatedRange() {
		return config.getSince() == null ? null : 
			new DateTimeRange(config.getSince(), config.start);
	}
	
	@Override
	public FieldNames getFieldNames() { 
		return config.getIncludeColumns(); 
	}
	
	@Override
	public Integer getPageSize() { 
		return config.getPageSize(); 
	}	

}
