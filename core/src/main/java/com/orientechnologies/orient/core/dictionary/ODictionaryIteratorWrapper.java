package com.orientechnologies.orient.core.dictionary;

import java.util.Iterator;
import java.util.Map.Entry;

import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.record.ORecordInternal;

public class ODictionaryIteratorWrapper implements Iterator<Entry<String, Object>> {
	private ODatabaseDocument														database;
	private Iterator<Entry<String, ORecordInternal<?>>>	wrapped;

	public ODictionaryIteratorWrapper(final ODatabaseDocument iDatabase, final Iterator<Entry<String, ORecordInternal<?>>> iterator) {
		database = iDatabase;
		wrapped = iterator;
	}

	public Entry<String, Object> next() {
		return next(null);
	}

	public Entry<String, Object> next(final String iFetchPlan) {
		final Entry<String, ? extends ORecordInternal<?>> entry = wrapped.next();
		return new OPair<String, Object>(entry.getKey(), database.getUserObjectByRecord(entry.getValue(), iFetchPlan));
	}

	public boolean hasNext() {
		return wrapped.hasNext();
	}

	public void remove() {
		wrapped.remove();
	}
}
