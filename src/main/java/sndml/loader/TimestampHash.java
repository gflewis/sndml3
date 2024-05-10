package sndml.loader;

import java.util.Hashtable;

import sndml.servicenow.*;
import sndml.util.DateTime;

@SuppressWarnings("serial")
public class TimestampHash extends Hashtable<RecordKey, DateTime> {

	KeySet getKeys() {
		return new KeySet(this.keySet());
	}

}
