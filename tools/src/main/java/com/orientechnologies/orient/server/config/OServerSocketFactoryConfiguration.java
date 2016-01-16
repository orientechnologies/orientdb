/*
 * Copyright 2014 Charles Baptiste (cbaptiste--at--blacksparkcorp.com)
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
package com.orientechnologies.orient.server.config;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElementRef;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

@XmlRootElement(name = "socket")
@XmlType(propOrder = { "parameters", "implementation", "name" })
public class OServerSocketFactoryConfiguration {

	public OServerSocketFactoryConfiguration() {
	}

	public OServerSocketFactoryConfiguration(String name, String implementation) {
		this.name = name;
		this.implementation = implementation;
	}

	@XmlAttribute(required = true)
	public String name;

	@XmlAttribute(required = true)
	public String implementation;

	@XmlElementWrapper
	@XmlElementRef(type = OServerParameterConfiguration.class)
	public OServerParameterConfiguration[] parameters;
}
