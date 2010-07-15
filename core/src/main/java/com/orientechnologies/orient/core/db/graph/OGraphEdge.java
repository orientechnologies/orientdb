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
public class OGraphEdge extends OGraphElement {
	public static final String	CLASS_NAME	= "OGraphEdge";
	public static final String	SOURCE			= "source";
	public static final String	DESTINATION	= "destination";

	private OGraphVertex				source;
	private OGraphVertex				destination;

	public OGraphEdge(final ODatabaseRecord<?> iDatabase, final ORID iRID) {
		super(iDatabase, iRID);
	}

	public OGraphEdge(final ODatabaseRecord<?> iDatabase) {
		super(iDatabase, CLASS_NAME);
	}

	public OGraphEdge(final ODatabaseRecord<?> database, final OGraphVertex iSourceNode, final OGraphVertex iDestinationNode) {
		this(database);
		source = iSourceNode;
		destination = iDestinationNode;
		set(SOURCE, iSourceNode.getDocument()).set(DESTINATION, iDestinationNode.getDocument());
	}

	public OGraphEdge(final ODocument iDocument) {
		super(iDocument);
		if (iDocument.getInternalStatus() == STATUS.NOT_LOADED)
			iDocument.load();
	}

	public OGraphVertex getSource() {
		if (source == null)
			source = new OGraphVertex((ODocument) document.field(SOURCE));

		return source;
	}

	public void setSource(final OGraphVertex iSource) {
		this.source = iSource;
		document.field(SOURCE, iSource.getDocument());
	}

	public OGraphVertex getDestination() {
		if (destination == null)
			destination = new OGraphVertex((ODocument) document.field(DESTINATION));

		return destination;
	}

	public void setDestination(final OGraphVertex iDestination) {
		this.destination = iDestination;
		document.field(DESTINATION, iDestination.getDocument());
	}
}
