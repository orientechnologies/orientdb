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
import java.util.List;

import com.orientechnologies.orient.core.annotation.OAfterDeserialization;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * GraphDB Edge class. It represent the edge (or arc) in the graph. The Edge can have custom properties. You can read/write them
 * using respectively get/set methods. The Edge is the connection between two Vertexes.
 * 
 * @see OGraphVertex
 */
public class OGraphEdge extends OGraphElement {
	public static final String					CLASS_NAME	= "OGraphEdge";
	public static final String					IN					= "in";
	public static final String					OUT					= "out";

	private SoftReference<OGraphVertex>	in;
	private SoftReference<OGraphVertex>	out;

	public OGraphEdge(final ODatabaseGraphTx iDatabase, final ORID iRID) {
		super(iDatabase, iRID);
	}

	public OGraphEdge(final ODatabaseGraphTx iDatabase) {
		super(iDatabase, CLASS_NAME);
	}

	public OGraphEdge(final ODatabaseGraphTx iDatabase, final String iClassName) {
		super(iDatabase, iClassName != null ? iClassName : CLASS_NAME);
	}

	public OGraphEdge(final ODatabaseGraphTx iDatabase, final String iClassName, final OGraphVertex iOutNode,
			final OGraphVertex iInNode) {
		this(iDatabase, iClassName);
		in = new SoftReference<OGraphVertex>(iInNode);
		out = new SoftReference<OGraphVertex>(iOutNode);
		set(IN, iInNode.getDocument());
		set(OUT, iOutNode.getDocument());
	}

	public OGraphEdge(final ODatabaseGraphTx iDatabase, final OGraphVertex iOutNode, final OGraphVertex iInNode) {
		this(iDatabase, CLASS_NAME, iOutNode, iInNode);
	}

	public OGraphEdge(final ODatabaseGraphTx iDatabase, final ODocument iDocument) {
		super(iDatabase, iDocument);
	}

	@Override
	@OAfterDeserialization
	public void fromStream(final ODocument iDocument) {
		super.fromStream(iDocument);
		document.setTrackingChanges(false);
		in = out = null;
	}

	public OGraphVertex getIn() {
		if (in == null || in.get() == null)
			in = new SoftReference<OGraphVertex>((OGraphVertex) database.getUserObjectByRecord((ODocument) document.field(IN), null));

		return in.get();
	}

	public OGraphVertex getOut() {
		if (out == null || out.get() == null)
			out = new SoftReference<OGraphVertex>((OGraphVertex) database.getUserObjectByRecord((ODocument) document.field(OUT), null));

		return out.get();
	}

	@Override
	public void reset() {
		document = null;
		in = null;
		out = null;
	}

	@Override
	public void delete() {
		delete(database, document);
		document = null;
		in = null;
		out = null;
	}

	public void onEvent(final ORecord<?> iDocument, final EVENT iEvent) {
		if (iEvent == EVENT.UNLOAD) {
			out = null;
			in = null;
		}
	}

	public static void delete(final ODatabaseGraphTx iDatabase, final ODocument iEdge) {
		final ODocument sourceVertex = (ODocument) iEdge.field(OUT);
		final ODocument targetVertex = (ODocument) iEdge.field(IN);

		List<OGraphEdge> edges;

		if (iDatabase.existsUserObjectByRID(sourceVertex.getIdentity())) {
			// WORK ALSO WITH IN MEMORY OBJECTS

			final OGraphVertex vertex = (OGraphVertex) iDatabase.getUserObjectByRecord(sourceVertex, null);
			// REMOVE THE EDGE OBJECT
			edges = vertex.getOutEdges();
			if (edges != null) {
				for (OGraphEdge e : edges)
					if (e.getDocument().equals(iEdge)) {
						edges.remove(e);
						break;
					}
			}
		}

		if (iDatabase.existsUserObjectByRID(targetVertex.getIdentity())) {
			// WORK ALSO WITH IN MEMORY OBJECTS

			final OGraphVertex vertex = (OGraphVertex) iDatabase.getUserObjectByRecord(targetVertex, null);
			// REMOVE THE EDGE OBJECT FROM THE TARGET VERTEX
			edges = vertex.getInEdges();
			if (edges != null) {
				for (OGraphEdge e : edges)
					if (e.getDocument().equals(iEdge)) {
						edges.remove(e);
						break;
					}
			}
		}

		// REMOVE THE EDGE DOCUMENT
		List<ODocument> docs = sourceVertex.field(OGraphVertex.FIELD_OUT_EDGES);
		if (docs != null)
			docs.remove(iEdge);

		sourceVertex.setDirty();
		sourceVertex.save();

		// REMOVE THE EDGE DOCUMENT FROM THE TARGET VERTEX
		docs = targetVertex.field(OGraphVertex.FIELD_IN_EDGES);
		if (docs != null)
			docs.remove(iEdge);

		targetVertex.setDirty();
		targetVertex.save();

		if (iDatabase.existsUserObjectByRID(iEdge.getIdentity())) {
			final OGraphEdge edge = (OGraphEdge) iDatabase.getUserObjectByRecord(iEdge, null);
			iDatabase.unregisterPojo(edge, iEdge);
		}

		iEdge.delete();
	}

	protected void setIn(final OGraphVertex iSource) {
		this.in = new SoftReference<OGraphVertex>(iSource);
		document.field(IN, iSource.getDocument());
	}

	protected void setOut(final OGraphVertex iDestination) {
		this.out = new SoftReference<OGraphVertex>(iDestination);
		document.field(OUT, iDestination.getDocument());
	}
}
