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

import java.util.ArrayList;
import java.util.List;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;

/**
 * GraphDB Node.
 */
public class ONode extends ODocument {
	public static final byte		RECORD_TYPE	= 'g';
	private static final String	CLASS_NAME	= "Node";

	public ONode(final ODatabaseRecord<?> iDatabase) {
		super(iDatabase, CLASS_NAME);
	}

	public Object get(final String iPropertyName) {
		return field(iPropertyName);
	}

	public ONode set(final String iPropertyName, final Object iPropertyValue) {
		return (ONode) super.field(iPropertyName, iPropertyValue, null);
	}

	public OArc link(final ONode iToNode) {
		if (iToNode == null)
			throw new IllegalArgumentException("Missed 'to' Arc property");

		final OArc arc = new OArc(getDatabase(), this, iToNode);

		List<ODocument> arcs = field("arcs");
		if (arcs == null) {
			arcs = new ArrayList<ODocument>();
			field("arcs", arcs);
		}

		arcs.add(arc);
		return arc;
	}

	public List<OArc> getLinks() {
		return field("arcs");
	}
}
