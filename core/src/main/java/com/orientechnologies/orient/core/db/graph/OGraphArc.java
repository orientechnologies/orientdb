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
package com.orientechnologies.orient.core.db.graph;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecord.STATUS;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * GraphDB Node.
 */
public class OGraphArc extends OGraphElement {
	private static final String	CLASS_NAME	= "OGraphArc";
	private static final String	SOURCE			= "source";
	private static final String	DESTINATION	= "destination";

	private OGraphNode					source;
	private OGraphNode					destination;

	public OGraphArc(final ODatabaseRecord<?> iDatabase, final ORID iRID) {
		super(iDatabase, iRID);
	}

	public OGraphArc(final ODatabaseRecord<?> iDatabase) {
		super(iDatabase, CLASS_NAME);
	}

	public OGraphArc(final ODatabaseRecord<?> database, final OGraphNode iSourceNode, final OGraphNode iDestinationNode) {
		this(database);
		set(SOURCE, iSourceNode.getDocument()).set(DESTINATION, iDestinationNode.getDocument());
	}

	public OGraphArc(final ODocument iDocument) {
		super(iDocument);
		if (iDocument.getInternalStatus() == STATUS.NOT_LOADED)
			iDocument.load();
	}

	public OGraphNode getSource() {
		if (source == null)
			source = new OGraphNode((ODocument) document.field(SOURCE));

		return source;
	}

	public OGraphNode getDestination() {
		if (destination == null)
			destination = new OGraphNode((ODocument) document.field(DESTINATION));

		return destination;
	}
}
