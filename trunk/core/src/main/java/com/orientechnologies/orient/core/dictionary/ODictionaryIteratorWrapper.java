package com.orientechnologies.orient.core.dictionary;

import java.util.Iterator;
import java.util.Map.Entry;

import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.db.vobject.ODatabaseVObject;
import com.orientechnologies.orient.core.record.impl.ORecordVObject;

public class ODictionaryIteratorWrapper implements Iterator<Entry<String, Object>> {
	private ODatabaseVObject										database;
	private ODictionaryIterator<ORecordVObject>	wrapped;

	public ODictionaryIteratorWrapper(final ODatabaseVObject iDatabase, final ODictionaryIterator<ORecordVObject> iToWrapper) {
		database = iDatabase;
		wrapped = iToWrapper;
	}

	public Entry<String, Object> next() {
		Entry<String, ORecordVObject> entry = wrapped.next();
		return new OPair<String, Object>(entry.getKey(), database.getUserObjectByRecord(entry.getValue()));
	}

	public boolean hasNext() {
		return wrapped.hasNext();
	}

	public void remove() {
		wrapped.remove();
	}
}
