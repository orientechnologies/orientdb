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
public class OGraphVertex extends OGraphElement {
	public static final String	CLASS_NAME	= "OGraphVertex";
	public static final String	FIELD_EDGES	= "edges";

	private List<OGraphEdge>		edges;

	public OGraphVertex(final ODatabaseGraphTx iDatabase, final ORID iRID) {
		super(new ODocument((ODatabaseRecord<?>) iDatabase.getUnderlying(), iRID));
	}

	public OGraphVertex(final ODatabaseGraphTx iDatabase) {
		super(new ODocument((ODatabaseRecord<?>) iDatabase.getUnderlying(), CLASS_NAME));
	}

	public OGraphVertex(final ODocument iDocument) {
		super(iDocument);
		if (iDocument.getInternalStatus() == STATUS.NOT_LOADED)
			iDocument.load();
	}

	public OGraphVertex(final ODocument iDocument, final String iFetchPlan) {
		super(iDocument);
		if (iDocument.getInternalStatus() == STATUS.NOT_LOADED)
			iDocument.load(iFetchPlan);
	}

	@SuppressWarnings("unchecked")
	public OGraphEdge link(final OGraphVertex iDestinationNode) {
		if (iDestinationNode == null)
			throw new IllegalArgumentException("Missed the arc destination property");

		final OGraphEdge arc = new OGraphEdge(document.getDatabase(), this, iDestinationNode);
		getEdges().add(arc);
		((List<ODocument>) document.field(FIELD_EDGES)).add(arc.getDocument());
		return arc;
	}

	/**
	 * Returns true if the vertex has at least one edge, otherwise false.
	 */
	public boolean hasEdges() {
		if (edges == null) {
			final List<ODocument> docs = document.field(FIELD_EDGES);
			return docs != null && !docs.isEmpty();
		}

		return !edges.isEmpty();
	}

	/**
	 * Returns the arcs of current node. If there are no arcs, then an empty list is returned.
	 */
	public List<OGraphEdge> getEdges() {
		if (edges == null) {
			edges = new ArrayList<OGraphEdge>();

			List<ODocument> docs = document.field(FIELD_EDGES);

			if (docs == null) {
				docs = new ArrayList<ODocument>();
				document.field(FIELD_EDGES, docs);
			} else {
				// TRANSFORM ALL THE ARCS
				for (ODocument d : docs) {
					edges.add(new OGraphEdge(d));
				}
			}
		}

		return edges;
	}

	@SuppressWarnings("unchecked")
	public List<OGraphVertex> browseEdgeDestinations() {
		final List<OGraphVertex> resultset = new ArrayList<OGraphVertex>();

		if (edges == null) {
			List<ODocument> docEdges = (List<ODocument>) document.field(FIELD_EDGES);

			// TRANSFORM ALL THE ARCS
			if (docEdges != null)
				for (ODocument d : docEdges) {
					resultset.add(new OGraphVertex((ODocument) d.field(OGraphEdge.DESTINATION)));
				}
		} else {
			for (OGraphEdge edge : edges) {
				resultset.add(edge.getDestination());
			}
		}

		return resultset;
	}
}
