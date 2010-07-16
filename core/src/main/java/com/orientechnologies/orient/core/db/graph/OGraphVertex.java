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
 * GraphDB Vertex class. It represent the vertex (or node) in the graph. The Vertex can have custom properties. You can read/write
 * them using respectively get/set methods. The Vertex can be connected to other Vertexes using edges. The Vertex keeps the edges
 * separated: "inEdges" for the incoming edges and "outEdges" for the outgoing edges.
 * 
 * @see OGraphEdge
 */
public class OGraphVertex extends OGraphElement {
	public static final String	CLASS_NAME			= "OGraphVertex";
	public static final String	FIELD_IN_EDGES	= "inEdges";
	public static final String	FIELD_OUT_EDGES	= "outEdges";

	private List<OGraphEdge>		inEdges;
	private List<OGraphEdge>		outEdges;

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

	/**
	 * Create a link between the current vertex and the target one.
	 * 
	 * @param iTargetVertex
	 *          Target vertex where to create the connection
	 * @return The new edge created
	 */
	@SuppressWarnings("unchecked")
	public OGraphEdge link(final OGraphVertex iTargetVertex) {
		if (iTargetVertex == null)
			throw new IllegalArgumentException("Missed the target vertex");

		// CREATE THE EDGE BETWEEN ME AND THE TARGET
		final OGraphEdge outEdge = new OGraphEdge(document.getDatabase(), this, iTargetVertex);
		getOutEdges().add(outEdge);
		((List<ODocument>) document.field(FIELD_OUT_EDGES)).add(outEdge.getDocument());

		// INSERT INTO THE INGOING EDGES OF TARGET
		final OGraphEdge inEdge = new OGraphEdge(document.getDatabase(), iTargetVertex, this);
		iTargetVertex.getInEdges().add(inEdge);
		((List<ODocument>) iTargetVertex.getDocument().field(FIELD_IN_EDGES)).add(inEdge.getDocument());

		return outEdge;
	}

	/**
	 * Remove the link between the current vertex and the target one.
	 * 
	 * @param iTargetVertex
	 *          Target vertex where to remove the connection
	 * @return Current vertex (useful for fluent calls)
	 */
	public OGraphVertex unlink(final OGraphVertex iTargetVertex) {
		if (iTargetVertex == null)
			throw new IllegalArgumentException("Missed the target vertex");

		// REMOVE THE EDGE OBJECT
		boolean found = false;
		if (outEdges != null) {
			for (OGraphEdge e : outEdges)
				if (e.getOut().equals(iTargetVertex)) {
					outEdges.remove(e);
					found = true;
					break;
				}

			if (!found)
				throw new IllegalArgumentException("Target vertex is not linked to the current vertex");
		}

		// REMOVE THE EDGE DOCUMENT
		List<ODocument> docs = document.field(FIELD_OUT_EDGES);
		docs.remove(iTargetVertex.getDocument());

		// REMOVE THE EDGE OBJECT FROM THE TARGET VERTEX
		if (iTargetVertex.inEdges != null) {
			for (OGraphEdge e : iTargetVertex.inEdges)
				if (e.getIn().equals(this)) {
					iTargetVertex.inEdges.remove(e);
					break;
				}
		}

		// REMOVE THE EDGE DOCUMENT FROM THE TARGET VERTEX
		docs = iTargetVertex.getDocument().field(FIELD_IN_EDGES);
		docs.remove(document);

		return this;
	}

	/**
	 * Returns true if the vertex has at least one incoming edge, otherwise false.
	 */
	public boolean hasInEdges() {
		if (inEdges == null) {
			final List<ODocument> docs = document.field(FIELD_IN_EDGES);
			return docs != null && !docs.isEmpty();
		}

		return !inEdges.isEmpty();
	}

	/**
	 * Returns true if the vertex has at least one outgoing edge, otherwise false.
	 */
	public boolean hasOutEdges() {
		if (outEdges == null) {
			final List<ODocument> docs = document.field(FIELD_OUT_EDGES);
			return docs != null && !docs.isEmpty();
		}

		return !outEdges.isEmpty();
	}

	/**
	 * Returns the incoming edges of current node. If there are no edged, then an empty list is returned.
	 */
	public List<OGraphEdge> getInEdges() {
		if (inEdges == null) {
			inEdges = new ArrayList<OGraphEdge>();

			List<ODocument> docs = document.field(FIELD_IN_EDGES);

			if (docs == null) {
				docs = new ArrayList<ODocument>();
				document.field(FIELD_IN_EDGES, docs);
			} else {
				// TRANSFORM ALL THE ARCS
				for (ODocument d : docs) {
					inEdges.add(new OGraphEdge(d));
				}
			}
		}

		return inEdges;
	}

	/**
	 * Returns the outgoing edges of current node. If there are no edged, then an empty list is returned.
	 */
	public List<OGraphEdge> getOutEdges() {
		if (outEdges == null) {
			outEdges = new ArrayList<OGraphEdge>();

			List<ODocument> docs = document.field(FIELD_OUT_EDGES);

			if (docs == null) {
				docs = new ArrayList<ODocument>();
				document.field(FIELD_OUT_EDGES, docs);
			} else {
				// TRANSFORM ALL THE ARCS
				for (ODocument d : docs) {
					outEdges.add(new OGraphEdge(d));
				}
			}
		}

		return outEdges;
	}

	/**
	 * Returns the list of Vertexes from the outgoing edges.
	 */
	@SuppressWarnings("unchecked")
	public List<OGraphVertex> browseOutEdgesVertexes() {
		final List<OGraphVertex> resultset = new ArrayList<OGraphVertex>();

		if (outEdges == null) {
			List<ODocument> docEdges = (List<ODocument>) document.field(FIELD_OUT_EDGES);

			// TRANSFORM ALL THE EDGES
			if (docEdges != null)
				for (ODocument d : docEdges) {
					resultset.add(new OGraphVertex((ODocument) d.field(OGraphEdge.OUT)));
				}
		} else {
			for (OGraphEdge edge : outEdges) {
				resultset.add(edge.getOut());
			}
		}

		return resultset;
	}

	/**
	 * Returns the list of Vertexes from the incoming edges.
	 */
	@SuppressWarnings("unchecked")
	public List<OGraphVertex> browseInEdgesVertexes() {
		final List<OGraphVertex> resultset = new ArrayList<OGraphVertex>();

		if (inEdges == null) {
			List<ODocument> docEdges = (List<ODocument>) document.field(FIELD_IN_EDGES);

			// TRANSFORM ALL THE EDGES
			if (docEdges != null)
				for (ODocument d : docEdges) {
					resultset.add(new OGraphVertex((ODocument) d.field(OGraphEdge.IN)));
				}
		} else {
			for (OGraphEdge edge : inEdges) {
				resultset.add(edge.getIn());
			}
		}

		return resultset;
	}
}
