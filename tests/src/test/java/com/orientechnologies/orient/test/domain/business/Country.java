package com.orientechnologies.orient.test.domain.business;

public class Country {
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
}
