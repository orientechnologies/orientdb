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
import com.orientechnologies.orient.core.id.ORID;

/**
 * GraphDB Node.
 */
public class ONode extends ODocument {
	private static final String	CLASS_NAME	= "Node";

	public ONode(final ODatabaseRecord<?> iDatabase, final ORID iRID) {
		super(iDatabase, iRID);
	}

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
		getArcs().add(arc);
		return arc;
	}

	/**
	 * Returns the arcs of current node. If there are no arcs, then an empty list is returned.
	 */
	public List<OArc> getArcs() {
		List<OArc> arcs = field("arcs");

		if (arcs == null) {
			arcs = new ArrayList<OArc>();
			field("arcs", arcs);
		}

		return arcs;
	}

	@Override
	public byte getRecordType() {
		return RECORD_TYPE;
	}

}
