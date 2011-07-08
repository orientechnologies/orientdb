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
package com.orientechnologies.orient.server.config;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAnyElement;
import javax.xml.bind.annotation.XmlElementRef;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "security")
public class OServerSecurityConfiguration {
	@XmlElementWrapper
	@XmlAnyElement
	@XmlElementRef(type = OServerUserConfiguration.class)
	public List<OServerUserConfiguration>			users;

	@XmlElementWrapper
	@XmlAnyElement
	@XmlElementRef(type = OServerNetworkListenerConfiguration.class)
	public List<OServerResourceConfiguration>	resources;

	public OServerSecurityConfiguration() {
	}

	public OServerSecurityConfiguration(Object iObject) {
		users = new ArrayList<OServerUserConfiguration>();
		resources = new ArrayList<OServerResourceConfiguration>();
	}
}
