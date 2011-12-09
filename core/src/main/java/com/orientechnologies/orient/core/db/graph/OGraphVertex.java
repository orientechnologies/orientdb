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

import java.lang.ref.SoftReference;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.orientechnologies.orient.core.annotation.OAfterDeserialization;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OGraphException;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.type.ODocumentWrapper;

/**
 * GraphDB Vertex class. It represent the vertex (or node) in the graph. The Vertex can have custom properties. You can read/write
 * them using respectively get/set methods. The Vertex can be connected to other Vertexes using edges. The Vertex keeps the edges
 * separated: "inEdges" for the incoming edges and "outEdges" for the outgoing edges.
 * 
 * @see OGraphEdge
 */
public class OGraphVertex extends OGraphElement implements Cloneable {
	private SoftReference<Set<OGraphEdge>>	in;
	private SoftReference<Set<OGraphEdge>>	out;

	public OGraphVertex(final ODatabaseGraphTx iDatabase) {
		super(iDatabase, OGraphDatabase.VERTEX_CLASS_NAME);
	}

	public OGraphVertex(final ODatabaseGraphTx iDatabase, final String iClassName) {
		super(iDatabase, iClassName != null ? iClassName : OGraphDatabase.VERTEX_CLASS_NAME);
	}

	public OGraphVertex(final ODatabaseGraphTx iDatabase, final ODocument iDocument) {
		super(iDatabase, iDocument);
	}

	public OGraphVertex(final ODatabaseGraphTx iDatabase, final ODocument iDocument, final String iFetchPlan) {
		super(iDatabase, iDocument);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <RET extends ODocumentWrapper> RET save() {
		super.save();
		if (database != null)
			database.registerUserObject(this, document);
		return (RET) this;
	}

	@Override
	public OGraphVertex clone() {
		return new OGraphVertex(database, document);
	}

	@Override
	@OAfterDeserialization
	public void fromStream(final ODocument iDocument) {
		super.fromStream(iDocument);
		document.setTrackingChanges(false);
		in = out = null;
	}

	/**
	 * Create a link between the current vertex and the target one. The link is of type iClassName.
	 * 
	 * @param iTargetVertex
	 *          Target vertex where to create the connection
	 * @param iClassName
	 *          The name of the class to use for the Edge
	 * @return The new edge created
	 */
	@SuppressWarnings("unchecked")
	public OGraphEdge link(final OGraphVertex iTargetVertex, final String iClassName) {
		if (iTargetVertex == null)
			throw new IllegalArgumentException("Missed the target vertex");

		// CREATE THE EDGE BETWEEN ME AND THE TARGET
		final OGraphEdge edge = new OGraphEdge(database, iClassName, this, iTargetVertex);
		getOutEdges().add(edge);

		Set<ODocument> recordEdges = ((Set<ODocument>) document.field(OGraphDatabase.VERTEX_FIELD_OUT));
		if (recordEdges == null) {
			recordEdges = new HashSet<ODocument>();
			document.field(OGraphDatabase.VERTEX_FIELD_OUT, recordEdges);
		}
		recordEdges.add(edge.getDocument());
		document.setDirty();

		// INSERT INTO THE INGOING EDGES OF TARGET
		iTargetVertex.getInEdges().add(edge);

		recordEdges = ((Set<ODocument>) iTargetVertex.getDocument().field(OGraphDatabase.VERTEX_FIELD_IN));
		if (recordEdges == null) {
			recordEdges = new HashSet<ODocument>();
			iTargetVertex.getDocument().field(OGraphDatabase.VERTEX_FIELD_IN, recordEdges);
		}

		recordEdges.add(edge.getDocument());
		iTargetVertex.getDocument().setDirty();

		return edge;
	}

	/**
	 * Create a link between the current vertex and the target one.
	 * 
	 * @param iTargetVertex
	 *          Target vertex where to create the connection
	 * @return The new edge created
	 */
	public OGraphEdge link(final OGraphVertex iTargetVertex) {
		return link(iTargetVertex, null);
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

		unlink(database, document, iTargetVertex.getDocument());

		return this;
	}

	/**
	 * Returns true if the vertex has at least one incoming edge, otherwise false.
	 */
	public boolean hasInEdges() {
		final Set<ODocument> docs = document.field(OGraphDatabase.VERTEX_FIELD_IN);
		return docs != null && !docs.isEmpty();
	}

	/**
	 * Returns true if the vertex has at least one outgoing edge, otherwise false.
	 */
	public boolean hasOutEdges() {
		final Set<ODocument> docs = document.field(OGraphDatabase.VERTEX_FIELD_OUT);
		return docs != null && !docs.isEmpty();
	}

	/**
	 * Returns the incoming edges of current node. If there are no edged, then an empty set is returned.
	 */
	public Set<OGraphEdge> getInEdges() {
		return getInEdges(null);
	}

	/**
	 * Returns the incoming edges of current node having the requested label. If there are no edged, then an empty set is returned.
	 */
	public Set<OGraphEdge> getInEdges(final String iEdgeLabel) {
		Set<OGraphEdge> temp = in != null ? in.get() : null;

		if (temp == null) {
			if (iEdgeLabel == null)
				temp = new HashSet<OGraphEdge>();
			in = new SoftReference<Set<OGraphEdge>>(temp);

			final Set<Object> docs = document.field(OGraphDatabase.VERTEX_FIELD_IN);

			if (docs != null) {
				// TRANSFORM ALL THE ARCS
				for (Object o : docs) {
					final ODocument doc = (ODocument) ((OIdentifiable) o).getRecord();
					if (iEdgeLabel != null && !iEdgeLabel.equals(doc.field(OGraphDatabase.LABEL)))
						continue;

					temp.add((OGraphEdge) database.getUserObjectByRecord(doc, null));
				}
			}
		} else if (iEdgeLabel != null) {
			// FILTER THE EXISTENT COLLECTION
			HashSet<OGraphEdge> filtered = new HashSet<OGraphEdge>();
			for (OGraphEdge e : temp) {
				if (iEdgeLabel.equals(e.getLabel()))
					filtered.add(e);
			}
			temp = filtered;
		}

		return temp;
	}

	/**
	 * Returns all the outgoing edges of current node. If there are no edged, then an empty set is returned.
	 */
	public Set<OGraphEdge> getOutEdges() {
		return getOutEdges(null);
	}

	/**
	 * Returns the outgoing edge of current node having the requested label. If there are no edged, then an empty set is returned.
	 */
	public Set<OGraphEdge> getOutEdges(final String iEdgeLabel) {
		Set<OGraphEdge> temp = out != null ? out.get() : null;

		if (temp == null) {
			temp = new HashSet<OGraphEdge>();
			if (iEdgeLabel == null)
				out = new SoftReference<Set<OGraphEdge>>(temp);

			final Set<Object> docs = document.field(OGraphDatabase.VERTEX_FIELD_OUT);

			if (docs != null) {
				// TRANSFORM ALL THE ARCS
				for (Object o : docs) {
					final ODocument doc = (ODocument) ((OIdentifiable) o).getRecord();
					if (iEdgeLabel != null && !iEdgeLabel.equals(doc.field(OGraphDatabase.LABEL)))
						continue;

					temp.add((OGraphEdge) database.getUserObjectByRecord(doc, null));
				}
			}
		} else if (iEdgeLabel != null) {
			// FILTER THE EXISTENT COLLECTION
			HashSet<OGraphEdge> filtered = new HashSet<OGraphEdge>();
			for (OGraphEdge e : temp) {
				if (iEdgeLabel.equals(e.getLabel()))
					filtered.add(e);
			}
			temp = filtered;
		}

		return temp;
	}

	/**
	 * Returns the set of Vertexes from the outgoing edges. It avoids to unmarshall edges.
	 */
	@SuppressWarnings("unchecked")
	public Set<OGraphVertex> browseOutEdgesVertexes() {
		final Set<OGraphVertex> resultset = new HashSet<OGraphVertex>();

		Set<OGraphEdge> temp = out != null ? out.get() : null;

		if (temp == null) {
			final Set<OIdentifiable> docEdges = (Set<OIdentifiable>) document.field(OGraphDatabase.VERTEX_FIELD_OUT);

			// TRANSFORM ALL THE EDGES
			if (docEdges != null)
				for (OIdentifiable d : docEdges) {
					resultset.add((OGraphVertex) database.getUserObjectByRecord(
							(ODocument) ((ODocument) d.getRecord()).field(OGraphDatabase.EDGE_FIELD_IN), null));
				}
		} else {
			for (OGraphEdge edge : temp) {
				resultset.add(edge.getIn());
			}
		}

		return resultset;
	}

	/**
	 * Returns the set of Vertexes from the incoming edges. It avoids to unmarshall edges.
	 */
	@SuppressWarnings("unchecked")
	public Set<OGraphVertex> browseInEdgesVertexes() {
		final Set<OGraphVertex> resultset = new HashSet<OGraphVertex>();

		Set<OGraphEdge> temp = in != null ? in.get() : null;

		if (temp == null) {
			final Set<ODocument> docEdges = (Set<ODocument>) document.field(OGraphDatabase.VERTEX_FIELD_IN);

			// TRANSFORM ALL THE EDGES
			if (docEdges != null)
				for (ODocument d : docEdges) {
					resultset.add((OGraphVertex) database.getUserObjectByRecord((ODocument) d.field(OGraphDatabase.EDGE_FIELD_OUT), null));
				}
		} else {
			for (OGraphEdge edge : temp) {
				resultset.add(edge.getOut());
			}
		}

		return resultset;
	}

	public ODocument findInVertex(final OGraphVertex iVertexDocument) {
		final Set<ODocument> docs = document.field(OGraphDatabase.VERTEX_FIELD_IN);
		if (docs == null || docs.size() == 0)
			return null;

		for (ODocument d : docs) {
			if (d.<ODocument> field(OGraphDatabase.EDGE_FIELD_IN).equals(iVertexDocument.getDocument()))
				return d;
		}

		return null;
	}

	public ODocument findOutVertex(final OGraphVertex iVertexDocument) {
		final Set<ODocument> docs = document.field(OGraphDatabase.VERTEX_FIELD_OUT);
		if (docs == null || docs.size() == 0)
			return null;

		for (ODocument d : docs) {
			if (d.<ODocument> field(OGraphDatabase.EDGE_FIELD_OUT).equals(iVertexDocument.getDocument()))
				return d;
		}

		return null;
	}

	public int getInEdgeCount() {
		final Set<ODocument> docs = document.field(OGraphDatabase.VERTEX_FIELD_IN);
		return docs == null ? 0 : docs.size();
	}

	public int getOutEdgeCount() {
		final Set<ODocument> docs = document.field(OGraphDatabase.VERTEX_FIELD_OUT);
		return docs == null ? 0 : docs.size();
	}

	@Override
	public void reset() {
		document = null;
		in = null;
		out = null;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void delete() {
		// DELETE ALL THE IN-OUT EDGES FROM RAM
		if (in != null && in.get() != null)
			in.get().clear();
		in = null;

		if (out != null && out.get() != null)
			out.get().clear();
		out = null;

		Set<ODocument> docs = (Set<ODocument>) document.field(OGraphDatabase.VERTEX_FIELD_IN);
		if (docs != null)
			for (ODocument doc : docs.toArray(new ODocument[docs.size()]))
				OGraphEdge.delete(database, doc);

		docs = (Set<ODocument>) document.field(OGraphDatabase.VERTEX_FIELD_OUT);
		if (docs != null)
			for (ODocument doc : docs.toArray(new ODocument[docs.size()]))
				OGraphEdge.delete(database, doc);

		database.unregisterPojo(this, document);

		document.delete();
		document = null;
	}

	public void onEvent(final ORecord<?> iDocument, final EVENT iEvent) {
		if (iEvent == EVENT.UNLOAD) {
			out = null;
			in = null;
		}
	}

	/**
	 * Unlinks all the edges between iSourceVertex and iTargetVertex
	 * 
	 * @param iDatabase
	 * @param iSourceVertex
	 * @param iTargetVertex
	 */
	public static void unlink(final ODatabaseGraphTx iDatabase, final ODocument iSourceVertex, final ODocument iTargetVertex) {
		if (iTargetVertex == null)
			throw new IllegalArgumentException("Missed the target vertex");

		if (iDatabase.existsUserObjectByRID(iSourceVertex.getIdentity())) {
			// WORK ALSO WITH IN MEMORY OBJECTS

			final OGraphVertex vertex = (OGraphVertex) iDatabase.getUserObjectByRecord(iSourceVertex, null);
			// REMOVE THE EDGE OBJECT
			if (vertex.out != null && vertex.out.get() != null) {
				for (OGraphEdge e : vertex.out.get())
					if (e.getIn().getDocument().equals(iTargetVertex))
						vertex.out.get().remove(e);
			}
		}

		if (iDatabase.existsUserObjectByRID(iTargetVertex.getIdentity())) {
			// WORK ALSO WITH IN MEMORY OBJECTS

			final OGraphVertex vertex = (OGraphVertex) iDatabase.getUserObjectByRecord(iTargetVertex, null);
			// REMOVE THE EDGE OBJECT FROM THE TARGET VERTEX
			if (vertex.in != null && vertex.in.get() != null) {
				for (OGraphEdge e : vertex.in.get())
					if (e.getOut().getDocument().equals(iSourceVertex))
						vertex.in.get().remove(e);
			}
		}

		final List<ODocument> edges2Remove = new ArrayList<ODocument>();

		// REMOVE THE EDGE DOCUMENT
		ODocument edge = null;
		Set<ODocument> docs = iSourceVertex.field(OGraphDatabase.VERTEX_FIELD_OUT);
		if (docs != null) {
			// USE A TEMP ARRAY TO AVOID CONCURRENT MODIFICATION TO THE ITERATOR
			for (OIdentifiable d : docs) {
				final ODocument doc = (ODocument) d.getRecord();
				if (doc.field(OGraphDatabase.EDGE_FIELD_IN).equals(iTargetVertex)) {
					edges2Remove.add(doc);
					edge = doc;
				}
			}

			for (ODocument d : edges2Remove)
				docs.remove(d);
		}

		if (edge == null)
			throw new OGraphException("Edge not found between the ougoing edges");

		iSourceVertex.setDirty();
		iSourceVertex.save();

		docs = iTargetVertex.field(OGraphDatabase.VERTEX_FIELD_IN);

		// REMOVE THE EDGE DOCUMENT FROM THE TARGET VERTEX
		if (docs != null) {
			edges2Remove.clear();

			for (OIdentifiable d : docs) {
				final ODocument doc = (ODocument) d.getRecord();
				if (doc.field(OGraphDatabase.EDGE_FIELD_IN).equals(iTargetVertex))
					edges2Remove.add(doc);
			}

			for (ODocument d : edges2Remove)
				docs.remove(d);
		}

		iTargetVertex.setDirty();
		iTargetVertex.save();

		edge.delete();
	}
}
