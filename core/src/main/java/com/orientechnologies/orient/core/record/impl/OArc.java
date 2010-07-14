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
package com.orientechnologies.orient.core.record.impl;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.id.ORID;

/**
 * GraphDB Node.
 */
public class OArc extends ODocument {
	private static final String	CLASS_NAME	= "Arc";
	private static final String	SOURCE			= "source";
	private static final String	DESTINATION	= "destination";

	public OArc(final ODatabaseRecord<?> iDatabase, final ORID iRID) {
		super(iDatabase, iRID);
	}

	public OArc(final ODatabaseRecord<?> iDatabase) {
		super(iDatabase, CLASS_NAME);
	}

	public OArc(ODatabaseRecord<?> database, final ONode iFromNode, final ONode iToNode) {
		this(database);
		set(SOURCE, this).set(DESTINATION, iToNode);
	}

	public ONode getSource() {
		return field(SOURCE);
	}

	public ONode getDestination() {
		return field(DESTINATION);
	}

	public Object get(final String iPropertyName) {
		return field(iPropertyName);
	}

	public OArc set(final String iPropertyName, final Object iPropertyValue) {
		return (OArc) super.field(iPropertyName, iPropertyValue, null);
	}
}
