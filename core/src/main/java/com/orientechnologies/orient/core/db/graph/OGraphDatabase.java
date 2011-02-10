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

import java.util.List;

import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.ORecordLazyList;
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
	private OClass							vertexBaseClass;
	private OClass							edgeBaseClass;

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

	public ODocument createEdge(final ODocument iSourceVertex, final ODocument iDestVertex, final String iClassName) {
		final boolean safeMode = beginBlock();

		try {
			final ODocument edge = new ODocument(this, iClassName != null ? iClassName : EDGE_CLASS_NAME);
			edge.field(EDGE_FIELD_IN, iSourceVertex);
			edge.field(EDGE_FIELD_OUT, iDestVertex);

			ORecordLazyList outEdges = ((ORecordLazyList) iSourceVertex.field(VERTEX_FIELD_OUT_EDGES));
			if (outEdges == null) {
				outEdges = new ORecordLazyList(underlying, ODocument.RECORD_TYPE);
				iSourceVertex.field(VERTEX_FIELD_OUT_EDGES, outEdges);
			}
			outEdges.add(edge);

			ORecordLazyList inEdges = ((ORecordLazyList) iDestVertex.field(VERTEX_FIELD_IN_EDGES));
			if (inEdges == null) {
				inEdges = new ORecordLazyList(underlying, ODocument.RECORD_TYPE);
				iDestVertex.field(VERTEX_FIELD_IN_EDGES, inEdges);
			}
			inEdges.add(edge);

			commitBlock(safeMode);

			return edge;

		} catch (RuntimeException e) {
			rollbackBlock(safeMode);
			throw e;
		}
	}

	public ORecordLazyList getOutEdges(final ODocument iVertex) {
		if (!iVertex.getSchemaClass().isSubClassOf(vertexBaseClass))
			throw new IllegalArgumentException("The document received is not a vertex");
		return iVertex.field(VERTEX_FIELD_OUT_EDGES);
	}

	public ORecordLazyList getInEdges(final ODocument iVertex) {
		if (!iVertex.getSchemaClass().isSubClassOf(vertexBaseClass))
			throw new IllegalArgumentException("The document received is not a vertex");
		return iVertex.field(VERTEX_FIELD_IN_EDGES);
	}

	public ODocument getInVertex(final ODocument iEdge) {
		if (!iEdge.getSchemaClass().isSubClassOf(edgeBaseClass))
			throw new IllegalArgumentException("The document received is not an edge");
		return iEdge.field(EDGE_FIELD_IN);
	}

	public ODocument getOutVertex(final ODocument iEdge) {
		if (!iEdge.getSchemaClass().isSubClassOf(edgeBaseClass))
			throw new IllegalArgumentException("The document received is not an edge");
		return iEdge.field(EDGE_FIELD_OUT);
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
		return createVertexType(iClassName, vertexBaseClass);
	}

	public OClass createVertexType(final String iClassName, OClass iSuperClass) {
		OClass cls = getMetadata().getSchema().createClass(iClassName).setSuperClass(iSuperClass);
		getMetadata().getSchema().save();
		return cls;
	}

	public OClass createVertexType(final String iClassName, final String iSuperClassName) {
		return createVertexType(iClassName, getMetadata().getSchema().getClass(iSuperClassName));
	}

	public OClass getVertexType(final String iClassName) {
		return getMetadata().getSchema().getClass(iClassName);
	}

	public OClass createEdgeType(final String iClassName) {
		OClass cls = getMetadata().getSchema().createClass(iClassName)
				.setSuperClass(getMetadata().getSchema().getClass(EDGE_CLASS_NAME));
		getMetadata().getSchema().save();
		return cls;
	}

	public OClass getEdgeType(final String iClassName) {
		return getMetadata().getSchema().getClass(iClassName);
	}

	private void checkForGraphSchema() {
		vertexBaseClass = getMetadata().getSchema().getClass(VERTEX_CLASS_NAME);
		edgeBaseClass = getMetadata().getSchema().getClass(EDGE_CLASS_NAME);

		if (vertexBaseClass == null) {
			// CREATE THE META MODEL USING THE ORIENT SCHEMA
			vertexBaseClass = getMetadata().getSchema().createClass(VERTEX_CLASS_NAME, addPhysicalCluster(VERTEX_CLASS_NAME));

			if (edgeBaseClass == null)
				edgeBaseClass = getMetadata().getSchema().createClass(EDGE_CLASS_NAME, addPhysicalCluster(EDGE_CLASS_NAME));

			vertexBaseClass.createProperty(VERTEX_FIELD_IN_EDGES, OType.LINKLIST, edgeBaseClass);
			vertexBaseClass.createProperty(VERTEX_FIELD_OUT_EDGES, OType.LINKLIST, edgeBaseClass);
			edgeBaseClass.createProperty(EDGE_FIELD_IN, OType.LINK, vertexBaseClass);
			edgeBaseClass.createProperty(EDGE_FIELD_OUT, OType.LINK, vertexBaseClass);
		}
		getMetadata().getSchema().save();
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
