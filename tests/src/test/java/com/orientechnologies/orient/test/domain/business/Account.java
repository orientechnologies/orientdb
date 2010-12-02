/*
 * Copyright 1999-2010 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.test.domain.business;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.persistence.Id;

import com.orientechnologies.orient.core.annotation.OAfterDeserialization;

public class Account {
	@Id
	private Object						rid;

	private int								id;
	private String						name;
	private String						surname;
	private Date							birthDate;
	private float							salary;
	private List<Address>			addresses		= new ArrayList<Address>();
	private transient boolean	initialized	= false;

	public Account() {
	}

	public Account(int iId, String iName, String iSurname) {
		this.id = iId;
		this.name = iName;
		this.surname = iSurname;
	}

	@OAfterDeserialization
	public void initialize() {
		initialized = true;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getSurname() {
		return surname;
	}

	public void setSurname(String surname) {
		this.surname = surname;
	}

	public List<Address> getAddresses() {
		return addresses;
	}

	public int getId() {
		return id;
	}

	public Date getBirthDate() {
		return birthDate;
	}

	public void setBirthDate(Date birthDate) {
		this.birthDate = birthDate;
	}

	public float getSalary() {
		return salary;
	}

	public void setSalary(float salary) {
		this.salary = salary;
	}

	public boolean isInitialized() {
		return initialized;
	}

	public Object getRid() {
		return rid;
	}
}
