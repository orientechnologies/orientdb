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

import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.tx.OTransactionNoTx;

/**
 * Super light GraphDB implementation on top of the underlying Document. The generated vertexes and edges are compatible with those
 * of ODatabaseGraphTx and TinkerPop Blueprints implementation. This class is the fastest and lightest but you have ODocument
 * instances and not regular ad-hoc POJO as for other implementations. You could use this one for bulk operations and the others for
 * regular graph access.
 * 
 * @author Luca Garulli
 * 
 */
public class OGraphDatabase extends ODatabaseDocumentTx {
	public static final String	VERTEX_CLASS_NAME				= "OGraphVertex";
	public static final String	VERTEX_FIELD_IN_EDGES		= "inEdges";
	public static final String	VERTEX_FIELD_OUT_EDGES	= "outEdges";

	public static final String	EDGE_CLASS_NAME					= "OGraphEdge";
	public static final String	EDGE_FIELD_IN						= "in";
	public static final String	EDGE_FIELD_OUT					= "out";

	private boolean							safeMode								= false;

	public OGraphDatabase(final String iURL) {
		super(iURL);
	}

	@Override
	@SuppressWarnings("unchecked")
	public <THISDB extends ODatabase> THISDB open(final String iUserName, final String iUserPassword) {
		super.open(iUserName, iUserPassword);
		checkForGraphSchema();
		return (THISDB) this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public <THISDB extends ODatabase> THISDB create() {
		super.create();
		checkForGraphSchema();
		return (THISDB) this;
	}

	public ODocument createVertex(final String iClassName) {
		return new ODocument(this, iClassName);
	}

	public ODocument createVertex() {
		return new ODocument(this, VERTEX_CLASS_NAME);
	}

	public ODocument createEdge(final ORID iSourceVertexRid, final ORID iDestVertexRid) {
		return createEdge(iSourceVertexRid, iDestVertexRid, null);
	}

	public ODocument createEdge(final ORID iSourceVertexRid, final ORID iDestVertexRid, final String iClassName) {
		final ODocument sourceVertex = load(iSourceVertexRid);
		if (sourceVertex == null)
			throw new IllegalArgumentException("Source vertex '" + iSourceVertexRid + "' doesn't exist");

		final ODocument destVertex = load(iDestVertexRid);
		if (destVertex == null)
			throw new IllegalArgumentException("Source vertex '" + iDestVertexRid + "' doesn't exist");

		return createEdge(sourceVertex, destVertex, iClassName);
	}

	@SuppressWarnings("unchecked")
	public void removeEdge(final ODocument iEdge) {
		final boolean safeMode = beginBlock();

		try {
			final ODocument sourceVertex = iEdge.field(EDGE_FIELD_IN);
			final ODocument destVertex = iEdge.field(EDGE_FIELD_OUT);

			final ORID edgeIdentity = iEdge.getIdentity();

			List<ODocument> outEdges = ((List<ODocument>) sourceVertex.field(VERTEX_FIELD_OUT_EDGES));
			for (int i = 0; i < outEdges.size(); ++i) {
				if (outEdges.get(i).getIdentity().equals(edgeIdentity)) {
					outEdges.remove(i);
					break;
				}
			}

			List<ODocument> inEdges = ((List<ODocument>) destVertex.field(VERTEX_FIELD_IN_EDGES));
			for (int i = 0; i < inEdges.size(); ++i) {
				if (inEdges.get(i).getIdentity().equals(edgeIdentity)) {
					inEdges.remove(i);
					break;
				}
			}

			iEdge.delete();
			sourceVertex.delete();
			destVertex.delete();

			commitBlock(safeMode);

		} catch (RuntimeException e) {
			rollbackBlock(safeMode);
			throw e;
		}
	}

	public ODocument createEdge(final ODocument iSourceVertex, final ODocument iDestVertex) {
		return createEdge(iSourceVertex, iDestVertex, null);
	}

	@SuppressWarnings("unchecked")
	public ODocument createEdge(final ODocument iSourceVertex, final ODocument iDestVertex, final String iClassName) {
		final boolean safeMode = beginBlock();

		try {
			final ODocument edge = new ODocument(this, iClassName != null ? iClassName : EDGE_CLASS_NAME);
			edge.field(EDGE_FIELD_IN, iSourceVertex);
			edge.field(EDGE_FIELD_OUT, iDestVertex);

			List<ODocument> outEdges = ((List<ODocument>) iSourceVertex.field(VERTEX_FIELD_OUT_EDGES));
			if (outEdges == null) {
				outEdges = new ArrayList<ODocument>();
				iSourceVertex.field(VERTEX_FIELD_OUT_EDGES, outEdges);
			}
			outEdges.add(edge);

			List<ODocument> inEdges = ((List<ODocument>) iDestVertex.field(VERTEX_FIELD_OUT_EDGES));
			if (inEdges == null) {
				inEdges = new ArrayList<ODocument>();
				iDestVertex.field(VERTEX_FIELD_OUT_EDGES, inEdges);
			}
			inEdges.add(edge);

			commitBlock(safeMode);

			return edge;

		} catch (RuntimeException e) {
			rollbackBlock(safeMode);
			throw e;
		}
	}

	public ODocument getRoot(final String iName) {
		return getDictionary().get(iName);
	}

	public ODocument getRoot(final String iName, final String iFetchPlan) {
		return getDictionary().get(iName, iFetchPlan);
	}

	public OGraphDatabase setRoot(final String iName, final ODocument iNode) {
		getDictionary().put(iName, iNode);
		return this;
	}

	public OClass createVertexType(final String iClassName) {
		OClass cls = getMetadata().getSchema().createClass(iClassName).setSuperClass(getMetadata().getSchema().getClass(VERTEX_CLASS_NAME));
		getMetadata().getSchema().save();
		return cls;
	}

	public OClass getVertexType(final String iClassName) {
		return getMetadata().getSchema().getClass(iClassName);
	}

	public OClass createEdgeType(final String iClassName) {
		OClass cls = getMetadata().getSchema().createClass(iClassName).setSuperClass(getMetadata().getSchema().getClass(EDGE_CLASS_NAME));
		getMetadata().getSchema().save();
		return cls;
	}

	public OClass getEdgeType(final String iClassName) {
		return getMetadata().getSchema().getClass(iClassName);
	}

	private void checkForGraphSchema() {
		if (!getMetadata().getSchema().existsClass(VERTEX_CLASS_NAME)) {
			// CREATE THE META MODEL USING THE ORIENT SCHEMA
			final OClass vertex = getMetadata().getSchema().createClass(VERTEX_CLASS_NAME, addPhysicalCluster(VERTEX_CLASS_NAME));
			final OClass edge = getMetadata().getSchema().createClass(EDGE_CLASS_NAME, addPhysicalCluster(EDGE_CLASS_NAME));

			edge.createProperty(EDGE_FIELD_IN, OType.LINK, vertex);
			edge.createProperty(EDGE_FIELD_OUT, OType.LINK, vertex);

			vertex.createProperty(VERTEX_FIELD_IN_EDGES, OType.LINKLIST, edge);
			vertex.createProperty(VERTEX_FIELD_OUT_EDGES, OType.LINKLIST, edge);

			getMetadata().getSchema().save();
		}
	}

	public boolean isSafeMode() {
		return safeMode;
	}

	public void setSafeMode(boolean safeMode) {
		this.safeMode = safeMode;
	}

	protected boolean beginBlock() {
		if (safeMode && !(getTransaction() instanceof OTransactionNoTx)) {
			begin();
			return true;
		}
		return false;
	}

	protected void commitBlock(final boolean iOpenTxInSafeMode) {
		if (iOpenTxInSafeMode)
			commit();
	}

	protected void rollbackBlock(final boolean iOpenTxInSafeMode) {
		if (iOpenTxInSafeMode)
			rollback();
	}
}
