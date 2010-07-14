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

import java.util.ArrayList;
import java.util.List;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecord.STATUS;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * GraphDB Node.
 */
public class OGraphNode extends OGraphElement {
	private static final String	CLASS_NAME	= "OGraphNode";

	private List<OGraphArc>			arcs;

	public OGraphNode(final ODatabaseRecord<?> iDatabase, final ORID iRID) {
		super(new ODocument(iDatabase, iRID));
	}

	public OGraphNode(final ODatabaseRecord<?> iDatabase) {
		super(new ODocument(iDatabase, CLASS_NAME));
	}

	public OGraphNode(final ODocument iDocument) {
		super(iDocument);
		if (iDocument.getInternalStatus() == STATUS.NOT_LOADED)
			iDocument.load();
	}

	public OGraphNode(final ODocument iDocument, final String iFetchPlan) {
		super(iDocument);
		if (iDocument.getInternalStatus() == STATUS.NOT_LOADED)
			iDocument.load(iFetchPlan);
	}

	@SuppressWarnings("unchecked")
	public OGraphArc link(final OGraphNode iDestinationNode) {
		if (iDestinationNode == null)
			throw new IllegalArgumentException("Missed the arc destination property");

		final OGraphArc arc = new OGraphArc(document.getDatabase(), this, iDestinationNode);
		getArcs().add(arc);
		((List<ODocument>) document.field("arcs")).add(arc.getDocument());
		return arc;
	}

	/**
	 * Returns the arcs of current node. If there are no arcs, then an empty list is returned.
	 */
	public List<OGraphArc> getArcs() {
		if (arcs == null) {
			arcs = new ArrayList<OGraphArc>();

			List<ODocument> docs = document.field("arcs");

			if (docs == null) {
				docs = new ArrayList<ODocument>();
				document.field("arcs", docs);
			} else {
				// TRANSFORM ALL THE ARCS
				for (ODocument d : docs) {
					arcs.add(new OGraphArc(d));
				}
			}
		}

		return arcs;
	}
}
