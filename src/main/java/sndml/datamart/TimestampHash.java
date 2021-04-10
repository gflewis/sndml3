package sndml.datamart;

import java.util.Hashtable;

import sndml.servicenow.*;

public class TimestampHash extends Hashtable<RecordKey, DateTime> {

	private static final long serialVersionUID = 4282304281964973453L;

	KeySet getKeys() {
		return new KeySet(this.keySet());
	}

}
