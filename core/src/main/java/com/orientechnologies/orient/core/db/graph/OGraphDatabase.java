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

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.ODatabaseRecordAbstract;
import com.orientechnologies.orient.core.db.record.ODatabaseRecordTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.iterator.ORecordIteratorClass;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.tx.OTransactionNoTx;
import com.orientechnologies.orient.core.type.tree.OMVRBTreeRIDSet;

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
	public static final String	VERTEX_FIELD_IN					= "in";
	public static final String	VERTEX_FIELD_IN_EDGES		= "inEdges";
	public static final String	VERTEX_FIELD_OUT				= "out";
	public static final String	VERTEX_FIELD_OUT_EDGES	= "outEdges";

	public static final String	EDGE_CLASS_NAME					= "OGraphEdge";
	public static final String	EDGE_FIELD_IN						= "in";
	public static final String	EDGE_FIELD_OUT					= "out";
	public static final String	LABEL										= "label";

	private boolean							useCustomTypes					= true;
	private boolean							safeMode								= false;
	protected OClass						vertexBaseClass;
	protected OClass						edgeBaseClass;

	public OGraphDatabase(final String iURL) {
		super(iURL);
	}

	public OGraphDatabase(final ODatabaseRecordTx iSource) {
		super(iSource);
		checkForGraphSchema();
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

	@Override
	public void close() {
		super.close();
		vertexBaseClass = null;
		edgeBaseClass = null;
	}

	public long countVertexes() {
		return countClass(VERTEX_CLASS_NAME);
	}

	public long countEdges() {
		return countClass(EDGE_CLASS_NAME);
	}

	public Iterable<ODocument> browseVertices() {
		return browseElements(VERTEX_CLASS_NAME, true);
	}

	public Iterable<ODocument> browseVertices(final boolean iPolymorphic) {
		return browseElements(VERTEX_CLASS_NAME, iPolymorphic);
	}

	public Iterable<ODocument> browseEdges() {
		return browseElements(EDGE_CLASS_NAME, true);
	}

	public Iterable<ODocument> browseEdges(final boolean iPolymorphic) {
		return browseElements(EDGE_CLASS_NAME, iPolymorphic);
	}

	public Iterable<ODocument> browseElements(final String iClass, final boolean iPolymorphic) {
		return new ORecordIteratorClass<ODocument>(this, (ODatabaseRecordAbstract) getUnderlying(), iClass, iPolymorphic);
	}

	public ODocument createVertex() {
		return createVertex(null);
	}

	public ODocument createVertex(String iClassName) {
		if (iClassName == null)
			iClassName = VERTEX_CLASS_NAME;
		else
			checkVertexClass(iClassName);

		return new ODocument(this, iClassName).setOrdered(true);
	}

	public ODocument createEdge(final ORID iSourceVertexRid, final ORID iDestVertexRid) {
		return createEdge(iSourceVertexRid, iDestVertexRid, null);
	}

	public ODocument createEdge(final ORID iSourceVertexRid, final ORID iDestVertexRid, final String iClassName) {
		final ODocument sourceVertex = load(iSourceVertexRid);
		if (sourceVertex == null)
			throw new IllegalArgumentException("Source vertex '" + iSourceVertexRid + "' does not exist");

		final ODocument destVertex = load(iDestVertexRid);
		if (destVertex == null)
			throw new IllegalArgumentException("Source vertex '" + iDestVertexRid + "' does not exist");

		return createEdge(sourceVertex, destVertex, iClassName);
	}

	public void removeVertex(final ODocument iVertex) {
		final boolean safeMode = beginBlock();

		try {
			ODocument otherVertex;
			Set<ODocument> otherEdges;

			// REMOVE OUT EDGES
			Set<ODocument> edges = iVertex.field(VERTEX_FIELD_OUT);
			if (edges != null) {
				for (ODocument edge : edges) {
					if (edge != null) {
						otherVertex = edge.field(EDGE_FIELD_IN);
						if (otherVertex != null) {
							otherEdges = otherVertex.field(VERTEX_FIELD_IN);
							if (otherEdges != null && otherEdges.remove(edge))
								save(otherVertex);
						}
						delete(edge);
					}
				}
				edges.clear();
				iVertex.field(VERTEX_FIELD_OUT, edges);
			}

			// REMOVE IN EDGES
			edges = iVertex.field(VERTEX_FIELD_IN);
			if (edges != null) {
				for (ODocument edge : edges) {
					if (edge != null) {
						otherVertex = edge.field(EDGE_FIELD_OUT);
						if (otherVertex != null) {
							otherEdges = otherVertex.field(VERTEX_FIELD_OUT);
							if (otherEdges != null && otherEdges.remove(edge))
								save(otherVertex);
						}
						delete(edge);
					}
				}
				edges.clear();
				iVertex.field(VERTEX_FIELD_IN, edges);
			}

			// DELETE VERTEX AS DOCUMENT
			delete(iVertex);

			commitBlock(safeMode);

		} catch (RuntimeException e) {
			rollbackBlock(safeMode);
			throw e;
		}
	}

	@SuppressWarnings("unchecked")
	public void removeEdge(final ODocument iEdge) {
		final boolean safeMode = beginBlock();

		try {
			final ODocument outVertex = iEdge.field(EDGE_FIELD_OUT);
			if (outVertex != null) {
				final Set<ODocument> out = ((Set<ODocument>) outVertex.field(VERTEX_FIELD_OUT));
				if (out != null)
					out.remove(iEdge);
			}

			final ODocument inVertex = iEdge.field(EDGE_FIELD_IN);
			if (inVertex != null) {
				final Set<ODocument> in = ((Set<ODocument>) inVertex.field(VERTEX_FIELD_IN));
				if (in != null)
					in.remove(iEdge);
			}

			delete(iEdge);

			if (outVertex != null)
				save(outVertex);
			if (inVertex != null)
				save(inVertex);

			commitBlock(safeMode);

		} catch (RuntimeException e) {
			rollbackBlock(safeMode);
			throw e;
		}
	}

	public ODocument createEdge(final ODocument iSourceVertex, final ODocument iDestVertex) {
		return createEdge(iSourceVertex, iDestVertex, null);
	}

	public ODocument createEdge(final ODocument iOutVertex, final ODocument iInVertex, final String iClassName) {
		if (iOutVertex == null)
			throw new IllegalArgumentException("iOutVertex is null");

		if (iInVertex == null)
			throw new IllegalArgumentException("iInVertex is null");

		checkEdgeClass(iClassName);

		final boolean safeMode = beginBlock();

		try {
			iInVertex.setDatabase(this);
			iOutVertex.setDatabase(this);

			final ODocument edge = new ODocument(this, iClassName != null ? iClassName : EDGE_CLASS_NAME).setOrdered(true);
			edge.field(EDGE_FIELD_OUT, iOutVertex);
			edge.field(EDGE_FIELD_IN, iInVertex);

			OMVRBTreeRIDSet out = ((OMVRBTreeRIDSet) iOutVertex.field(VERTEX_FIELD_OUT));
			if (out == null) {
				out = new OMVRBTreeRIDSet(iOutVertex);
				iOutVertex.field(VERTEX_FIELD_OUT, out);
			}
			out.add(edge);

			OMVRBTreeRIDSet in = ((OMVRBTreeRIDSet) iInVertex.field(VERTEX_FIELD_IN));
			if (in == null) {
				in = new OMVRBTreeRIDSet(iInVertex);
				iInVertex.field(VERTEX_FIELD_IN, in);
			}
			in.add(edge);

			if (safeMode) {
				save(edge);
				commitBlock(safeMode);
			}

			return edge;

		} catch (RuntimeException e) {
			rollbackBlock(safeMode);
			throw e;
		}
	}

	/**
	 * Returns all the edges between the vertexes iVertex1 and iVertex2.
	 * 
	 * @param iVertex1
	 *          First Vertex
	 * @param iVertex2
	 *          Second Vertex
	 * @return The Set with the common Edges between the two vertexes. If edges aren't found the set is empty
	 */
	public Set<ODocument> getEdgesBetweenVertexes(final ODocument iVertex1, final ODocument iVertex2) {
		return getEdgesBetweenVertexes(iVertex1, iVertex2, null, null);
	}

	/**
	 * Returns all the edges between the vertexes iVertex1 and iVertex2 with label between the array of labels passed as iLabels.
	 * 
	 * @param iVertex1
	 *          First Vertex
	 * @param iVertex2
	 *          Second Vertex
	 * @param iLabels
	 *          Array of strings with the labels to get as filter
	 * @return The Set with the common Edges between the two vertexes. If edges aren't found the set is empty
	 */
	public Set<ODocument> getEdgesBetweenVertexes(final ODocument iVertex1, final ODocument iVertex2, final String[] iLabels) {
		return getEdgesBetweenVertexes(iVertex1, iVertex2, iLabels, null);
	}

	/**
	 * Returns all the edges between the vertexes iVertex1 and iVertex2 with label between the array of labels passed as iLabels and
	 * with class between the array of class names passed as iClassNames.
	 * 
	 * @param iVertex1
	 *          First Vertex
	 * @param iVertex2
	 *          Second Vertex
	 * @param iLabels
	 *          Array of strings with the labels to get as filter
	 * @param iClassNames
	 *          Array of strings with the name of the classes to get as filter
	 * @return The Set with the common Edges between the two vertexes. If edges aren't found the set is empty
	 */
	public Set<ODocument> getEdgesBetweenVertexes(final ODocument iVertex1, final ODocument iVertex2, final String[] iLabels,
			final String[] iClassNames) {

		final Set<ODocument> result = new HashSet<ODocument>();

		if (iVertex1 != null && iVertex2 != null) {
			// CHECK OUT EDGES
			for (OIdentifiable e : getOutEdges(iVertex1)) {
				final ODocument edge = (ODocument) e;

				if (checkEdge(edge, iLabels, iClassNames)) {
					if (edge.<ODocument> field("in").equals(iVertex2))
						result.add(edge);
				}
			}

			// CHECK IN EDGES
			for (OIdentifiable e : getInEdges(iVertex1)) {
				final ODocument edge = (ODocument) e;

				if (checkEdge(edge, iLabels, iClassNames)) {
					if (edge.<ODocument> field("out").equals(iVertex2))
						result.add(edge);
				}
			}
		}

		return result;
	}

	public Set<OIdentifiable> getOutEdges(final ODocument iVertex) {
		return getOutEdges(iVertex, null);
	}

	/**
	 * Retrieves the outgoing edges of vertex iVertex having label equals to iLabel.
	 * 
	 * @param iVertex
	 *          Target vertex
	 * @param iLabel
	 *          Label to search
	 * @return
	 */
	public Set<OIdentifiable> getOutEdges(final ODocument iVertex, final String iLabel) {
		checkVertexClass(iVertex);

		final OMVRBTreeRIDSet set = iVertex.field(VERTEX_FIELD_OUT);

		if (iLabel == null)
			// RETURN THE ENTIRE COLLECTION
			if (set != null)
				return Collections.unmodifiableSet(set);
			else
				return Collections.emptySet();

		// FILTER BY LABEL
		final OMVRBTreeRIDSet result = new OMVRBTreeRIDSet();
		if (set != null)
			for (OIdentifiable item : set) {
				if (iLabel == null || iLabel.equals(((ODocument) item).field(LABEL)))
					result.add(item);
			}

		return result;
	}

	/**
	 * Retrieves the outgoing edges of vertex iVertex having the requested properties iProperties set to the passed values
	 * 
	 * @param iVertex
	 *          Target vertex
	 * @param iProperties
	 *          Map where keys are property names and values the expected values
	 * @return
	 */
	public Set<OIdentifiable> getOutEdgesHavingProperties(final ODocument iVertex, final Map<String, Object> iProperties) {
		checkVertexClass(iVertex);

		return filterEdgesByProperties((OMVRBTreeRIDSet) iVertex.field(VERTEX_FIELD_OUT), iProperties);
	}

	/**
	 * Retrieves the outgoing edges of vertex iVertex having the requested properties iProperties
	 * 
	 * @param iVertex
	 *          Target vertex
	 * @param iProperties
	 *          Map where keys are property names and values the expected values
	 * @return
	 */
	public Set<OIdentifiable> getOutEdgesHavingProperties(final ODocument iVertex, Iterable<String> iProperties) {
		checkVertexClass(iVertex);

		return filterEdgesByProperties((OMVRBTreeRIDSet) iVertex.field(VERTEX_FIELD_OUT), iProperties);
	}

	public Set<OIdentifiable> getInEdges(final ODocument iVertex) {
		return getInEdges(iVertex, null);
	}

	public Set<OIdentifiable> getInEdges(final ODocument iVertex, final String iLabel) {
		checkVertexClass(iVertex);

		final OMVRBTreeRIDSet set = iVertex.field(VERTEX_FIELD_IN);

		if (iLabel == null)
			// RETURN THE ENTIRE COLLECTION
			if (set != null)
				return Collections.unmodifiableSet(set);
			else
				return Collections.emptySet();

		// FILTER BY LABEL
		final OMVRBTreeRIDSet result = new OMVRBTreeRIDSet();
		if (set != null)
			for (OIdentifiable item : set) {
				if (iLabel == null || iLabel.equals(((ODocument) item).field(LABEL)))
					result.add(item);
			}

		return result;
	}

	/**
	 * Retrieves the incoming edges of vertex iVertex having the requested properties iProperties
	 * 
	 * @param iVertex
	 *          Target vertex
	 * @param iProperties
	 *          Map where keys are property names and values the expected values
	 * @return
	 */
	public Set<OIdentifiable> getInEdgesHavingProperties(final ODocument iVertex, Iterable<String> iProperties) {
		checkVertexClass(iVertex);

		return filterEdgesByProperties((OMVRBTreeRIDSet) iVertex.field(VERTEX_FIELD_IN), iProperties);
	}

	/**
	 * Retrieves the incoming edges of vertex iVertex having the requested properties iProperties set to the passed values
	 * 
	 * @param iVertex
	 *          Target vertex
	 * @param iProperties
	 *          Map where keys are property names and values the expected values
	 * @return
	 */
	public Set<OIdentifiable> getInEdgesHavingProperties(final ODocument iVertex, final Map<String, Object> iProperties) {
		checkVertexClass(iVertex);
		return filterEdgesByProperties((OMVRBTreeRIDSet) iVertex.field(VERTEX_FIELD_IN), iProperties);
	}

	public ODocument getInVertex(final ODocument iEdge) {
		checkEdgeClass(iEdge);
		OIdentifiable v = iEdge.field(EDGE_FIELD_IN);
		if (v != null && v instanceof ORID) {
			// REPLACE WITH THE DOCUMENT
			v = v.getRecord();
			final boolean wasDirty = iEdge.isDirty();
			iEdge.field(EDGE_FIELD_IN, v);
			if (!wasDirty)
				iEdge.unsetDirty();
		}

		return (ODocument) v;
	}

	public ODocument getOutVertex(final ODocument iEdge) {
		checkEdgeClass(iEdge);
		OIdentifiable v = iEdge.field(EDGE_FIELD_OUT);
		if (v != null && v instanceof ORID) {
			// REPLACE WITH THE DOCUMENT
			v = v.getRecord();
			final boolean wasDirty = iEdge.isDirty();
			iEdge.field(EDGE_FIELD_OUT, v);
			if (!wasDirty)
				iEdge.unsetDirty();
		}

		return (ODocument) v;
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
		return getMetadata().getSchema().createClass(iClassName, vertexBaseClass);
	}

	public OClass createVertexType(final String iClassName, final String iSuperClassName) {
		checkVertexClass(iSuperClassName);
		return getMetadata().getSchema().createClass(iClassName, getMetadata().getSchema().getClass(iSuperClassName));
	}

	public OClass createVertexType(final String iClassName, final OClass iSuperClass) {
		checkVertexClass(iSuperClass);
		return getMetadata().getSchema().createClass(iClassName, iSuperClass);
	}

	public OClass getVertexType(final String iClassName) {
		return getMetadata().getSchema().getClass(iClassName);
	}

	public OClass createEdgeType(final String iClassName) {
		return getMetadata().getSchema().createClass(iClassName, edgeBaseClass);
	}

	public OClass createEdgeType(final String iClassName, final String iSuperClassName) {
		checkEdgeClass(iSuperClassName);
		return getMetadata().getSchema().createClass(iClassName, getMetadata().getSchema().getClass(iSuperClassName));
	}

	public OClass createEdgeType(final String iClassName, final OClass iSuperClass) {
		checkEdgeClass(iSuperClass);
		return getMetadata().getSchema().createClass(iClassName, iSuperClass);
	}

	public OClass getEdgeType(final String iClassName) {
		return getMetadata().getSchema().getClass(iClassName);
	}

	public boolean isSafeMode() {
		return safeMode;
	}

	public void setSafeMode(boolean safeMode) {
		this.safeMode = safeMode;
	}

	public OClass getVertexBaseClass() {
		return vertexBaseClass;
	}

	public OClass getEdgeBaseClass() {
		return edgeBaseClass;
	}

	public Set<OIdentifiable> filterEdgesByProperties(final OMVRBTreeRIDSet iEdges, final Iterable<String> iPropertyNames) {
		if (iPropertyNames == null)
			// RETURN THE ENTIRE COLLECTION
			if (iEdges != null)
				return Collections.unmodifiableSet(iEdges);
			else
				return Collections.emptySet();

		// FILTER BY PROPERTY VALUES
		final OMVRBTreeRIDSet result = new OMVRBTreeRIDSet();
		if (iEdges != null)
			for (OIdentifiable item : iEdges) {
				final ODocument doc = (ODocument) item;
				for (String propName : iPropertyNames) {
					if (doc.containsField(propName))
						// FOUND: ADD IT
						result.add(item);
				}
			}

		return result;
	}

	public Set<OIdentifiable> filterEdgesByProperties(final OMVRBTreeRIDSet iEdges, final Map<String, Object> iProperties) {
		if (iProperties == null)
			// RETURN THE ENTIRE COLLECTION
			if (iEdges != null)
				return Collections.unmodifiableSet(iEdges);
			else
				return Collections.emptySet();

		// FILTER BY PROPERTY VALUES
		final OMVRBTreeRIDSet result = new OMVRBTreeRIDSet();
		if (iEdges != null)
			for (OIdentifiable item : iEdges) {
				final ODocument doc = (ODocument) item;
				for (Entry<String, Object> prop : iProperties.entrySet()) {
					if (prop.getKey() != null && doc.containsField(prop.getKey())) {
						if (prop.getValue() == null) {
							if (doc.field(prop.getKey()) == null)
								// BOTH NULL: ADD IT
								result.add(item);
						} else if (prop.getValue().equals(doc.field(prop.getKey())))
							// SAME VALUE: ADD IT
							result.add(item);
					}
				}
			}

		return result;
	}

	public void checkVertexClass(final ODocument iVertex) {
		if (useCustomTypes && !iVertex.getSchemaClass().isSubClassOf(vertexBaseClass))
			throw new IllegalArgumentException("The document received is not a vertex. Found class '" + iVertex.getSchemaClass() + "'");
	}

	public void checkVertexClass(final String iVertexTypeName) {
		if (useCustomTypes && iVertexTypeName != null) {
			final OClass cls = getMetadata().getSchema().getClass(iVertexTypeName);
			if (cls == null)
				throw new IllegalArgumentException("The class '" + iVertexTypeName + "' was not found");

			if (!cls.isSubClassOf(vertexBaseClass))
				throw new IllegalArgumentException("The class '" + iVertexTypeName + "' does not extend the vertex type");
		}
	}

	public void checkVertexClass(final OClass iVertexType) {
		if (useCustomTypes && iVertexType != null) {
			if (!iVertexType.isSubClassOf(vertexBaseClass))
				throw new IllegalArgumentException("The class '" + iVertexType + "' does not extend the vertex type");
		}
	}

	public void checkEdgeClass(final ODocument iEdge) {
		if (useCustomTypes && !iEdge.getSchemaClass().isSubClassOf(edgeBaseClass))
			throw new IllegalArgumentException("The document received is not an edge. Found class '" + iEdge.getSchemaClass() + "'");
	}

	public void checkEdgeClass(final String iEdgeTypeName) {
		if (useCustomTypes && iEdgeTypeName != null) {
			final OClass cls = getMetadata().getSchema().getClass(iEdgeTypeName);
			if (cls == null)
				throw new IllegalArgumentException("The class '" + iEdgeTypeName + "' was not found");

			if (!cls.isSubClassOf(edgeBaseClass))
				throw new IllegalArgumentException("The class '" + iEdgeTypeName + "' does not extend the edge type");
		}
	}

	public void checkEdgeClass(final OClass iEdgeType) {
		if (useCustomTypes && iEdgeType != null) {
			if (!iEdgeType.isSubClassOf(edgeBaseClass))
				throw new IllegalArgumentException("The class '" + iEdgeType + "' does not extend the edge type");
		}
	}

	public boolean isUseCustomTypes() {
		return useCustomTypes;
	}

	public void setUseCustomTypes(boolean useCustomTypes) {
		this.useCustomTypes = useCustomTypes;
	}

	/**
	 * Returns true if the document is a vertex (its class is OGraphVertex or any subclasses)
	 * 
	 * @param iRecord
	 *          Document to analyze.
	 * @return true if the document is a vertex (its class is OGraphVertex or any subclasses)
	 */
	public boolean isVertex(final ODocument iRecord) {
		return iRecord != null ? iRecord.getSchemaClass().isSubClassOf(vertexBaseClass) : false;
	}

	/**
	 * Returns true if the document is an edge (its class is OGraphEdge or any subclasses)
	 * 
	 * @param iRecord
	 *          Document to analyze.
	 * @return true if the document is a edge (its class is OGraphEdge or any subclasses)
	 */
	public boolean isEdge(final ODocument iRecord) {
		return iRecord != null ? iRecord.getSchemaClass().isSubClassOf(edgeBaseClass) : false;
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

	protected boolean checkEdge(final ODocument iEdge, final String[] iLabels, final String[] iClassNames) {
		boolean good = true;

		if (iClassNames != null) {
			// CHECK AGAINST CLASS NAMES
			good = false;
			for (String c : iClassNames) {
				if (c.equals(iEdge.getClassName())) {
					good = true;
					break;
				}
			}
		}

		if (good && iLabels != null) {
			// CHECK AGAINST LABELS
			good = false;
			for (String c : iLabels) {
				if (c.equals(iEdge.field(LABEL))) {
					good = true;
					break;
				}
			}
		}
		return good;
	}

	public void checkForGraphSchema() {
		vertexBaseClass = getMetadata().getSchema().getClass(VERTEX_CLASS_NAME);
		edgeBaseClass = getMetadata().getSchema().getClass(EDGE_CLASS_NAME);

		if (vertexBaseClass == null) {
			// CREATE THE META MODEL USING THE ORIENT SCHEMA
			vertexBaseClass = getMetadata().getSchema().createClass(VERTEX_CLASS_NAME);
			vertexBaseClass.setShortName("V");
			vertexBaseClass.setOverSize(2);

			if (edgeBaseClass == null) {
				edgeBaseClass = getMetadata().getSchema().createClass(EDGE_CLASS_NAME);
				edgeBaseClass.setShortName("E");
			}

			vertexBaseClass.createProperty(VERTEX_FIELD_IN, OType.LINKSET, edgeBaseClass);
			vertexBaseClass.createProperty(VERTEX_FIELD_OUT, OType.LINKSET, edgeBaseClass);
			edgeBaseClass.createProperty(EDGE_FIELD_IN, OType.LINK, vertexBaseClass);
			edgeBaseClass.createProperty(EDGE_FIELD_OUT, OType.LINK, vertexBaseClass);
		} else {
			// @COMPATIBILITY <= 1.0rc4: CHANGE FROM outEdges -> out and inEdges -> in
			if (vertexBaseClass.existsProperty(OGraphDatabase.VERTEX_FIELD_OUT_EDGES)) {
				OGraphDatabaseMigration.migrate(this);
			}
		}
	}
}
