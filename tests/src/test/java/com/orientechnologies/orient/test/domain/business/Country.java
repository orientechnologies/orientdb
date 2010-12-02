package com.orientechnologies.orient.test.domain.business;

import com.orientechnologies.orient.core.annotation.OId;

public class Country {
	@OId
	private Object	id;
	private String	name;

	public Country() {
	}

	public Country(String iName) {
		name = iName;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Object getId() {
		return id;
	}
}
