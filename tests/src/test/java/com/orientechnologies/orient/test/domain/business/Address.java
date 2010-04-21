package com.orientechnologies.orient.test.domain.business;

public class Address {
	private String	type;
	private String	street;
	private City		city;

	public Address() {
	}

	public Address(String iType, City iCity, String iStreet) {
		type = iType;
		city = iCity;
		street = iStreet;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getStreet() {
		return street;
	}

	public void setStreet(String street) {
		this.street = street;
	}

	public City getCity() {
		return city;
	}

	public void setCity(City city) {
		this.city = city;
	}
}
