package com.orientechnologies.orient.test.domain.whiz;

import com.orientechnologies.orient.core.annotation.OId;

import java.util.Collection;

/**
 * Data object that collects things.
 */
public class Collector {
	@OId
	private String id;
	private Collection<String> stringCollection;

	public String getId() {
		return id;
	}

	public Collection<String> getStringCollection() {
		return stringCollection;
	}

	public void setStringCollection(Collection<String> stringCollection) {
		this.stringCollection = stringCollection;
	}
}
