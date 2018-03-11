package servicenow.datamart;

import servicenow.api.*;

import java.util.Hashtable;

public class TimestampHash extends Hashtable<Key, DateTime> {

	private static final long serialVersionUID = 4282304281964973453L;

	KeySet getKeys() {
		return new KeySet(this.keySet());
	}

}
