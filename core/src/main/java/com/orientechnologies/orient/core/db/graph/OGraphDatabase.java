/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.ODatabaseRecordAbstract;
import com.orientechnologies.orient.core.db.record.ODatabaseRecordTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridset.sbtree.OSBTreeRIDSet;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.iterator.ORecordIteratorClass;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.OStorageEmbedded;
import com.orientechnologies.orient.core.tx.OTransactionNoTx;
import com.orientechnologies.orient.core.type.tree.OMVRBTreeRIDSet;
import com.orientechnologies.orient.core.type.tree.provider.OMVRBTreeRIDProvider;

/**
 * Super light GraphDB implementation on top of the underlying Document. The generated vertexes and edges are compatible with those
 * of ODatabaseGraphTx and TinkerPop Blueprints implementation. This class is the fastest and lightest but you have ODocument
 * instances and not regular ad-hoc POJO as for other implementations. You could use this one for bulk operations and the others for
 * regular graph access.
 * 
 * This class has been deprecated strating from v1.4 in favor of TinkerPop Blueprints OrientGraph and OrientGraphNoTx classes. Take
 * a look at: <a href="https://github.com/orientechnologies/orientdb/wiki/Migration-from-1.3.x-to-1.4.x#graphdb">Migration from
 * 1.3.x to 1.4.x</a>
 * 
 * @author Luca Garulli
 * @see OrientGraph, OrientGraphNoTx
 * @deprecated
 * 
 */
@Deprecated
public class OGraphDatabase extends ODatabaseDocumentTx {
  private final boolean preferSBTreeSet = OGlobalConfiguration.PREFER_SBTREE_SET.getValueAsBoolean();

  public enum LOCK_MODE {
    NO_LOCKING, DATABASE_LEVEL_LOCKING, RECORD_LEVEL_LOCKING
  }

  public enum DIRECTION {
    BOTH, IN, OUT
  }

  public static final String TYPE                 = "graph";

  public static final String VERTEX_CLASS_NAME    = "OGraphVertex";
  public static final String VERTEX_ALIAS         = "V";
  public static final String VERTEX_FIELD_IN      = "in_";
  public static final String VERTEX_FIELD_OUT     = "out_";
  public static final String VERTEX_FIELD_IN_OLD  = "in";
  public static final String VERTEX_FIELD_OUT_OLD = "out";

  public static final String EDGE_CLASS_NAME      = "OGraphEdge";
  public static final String EDGE_ALIAS           = "E";
  public static final String EDGE_FIELD_IN        = "in";
  public static final String EDGE_FIELD_OUT       = "out";
  public static final String LABEL                = "label";

  private String             outV                 = VERTEX_FIELD_OUT;
  private String             inV                  = VERTEX_FIELD_IN;
  private boolean            useCustomTypes       = true;
  private boolean            safeMode             = false;
  private LOCK_MODE          lockMode             = LOCK_MODE.NO_LOCKING;
  private boolean            retroCompatibility   = false;
  protected OClass           vertexBaseClass;
  protected OClass           edgeBaseClass;

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
    return countClass(VERTEX_ALIAS);
  }

  public long countEdges() {
    return countClass(EDGE_ALIAS);
  }

  public Iterable<ODocument> browseVertices() {
    return browseElements(VERTEX_ALIAS, true);
  }

  public Iterable<ODocument> browseVertices(final boolean iPolymorphic) {
    return browseElements(VERTEX_ALIAS, iPolymorphic);
  }

  public Iterable<ODocument> browseEdges() {
    return browseElements(EDGE_ALIAS, true);
  }

  public Iterable<ODocument> browseEdges(final boolean iPolymorphic) {
    return browseElements(EDGE_ALIAS, iPolymorphic);
  }

  public Iterable<ODocument> browseElements(final String iClass, final boolean iPolymorphic) {
    return new ORecordIteratorClass<ODocument>(this, (ODatabaseRecordAbstract) getUnderlying(), iClass, iPolymorphic, true, false);
  }

  public ODocument createVertex() {
    return createVertex(null);
  }

  public ODocument createVertex(final String iClassName) {
    return createVertex(iClassName, (Object[]) null);
  }

  @SuppressWarnings("unchecked")
  public ODocument createVertex(final String iClassName, final Object... iFields) {
    final OClass cls = checkVertexClass(iClassName);

    final ODocument vertex = new ODocument(cls).setOrdered(true);

    if (iFields != null)
      // SET THE FIELDS
      if (iFields != null)
        if (iFields.length == 1) {
          Object f = iFields[0];
          if (f instanceof Map<?, ?>)
            vertex.fields((Map<String, Object>) f);
          else
            throw new IllegalArgumentException(
                "Invalid fields: expecting a pairs of fields as String,Object or a single Map<String,Object>, but found: " + f);
        } else
          // SET THE FIELDS
          for (int i = 0; i < iFields.length; i += 2)
            vertex.field(iFields[i].toString(), iFields[i + 1]);

    return vertex;
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

  public ODocument createEdge(final ODocument iSourceVertex, final ODocument iDestVertex) {
    return createEdge(iSourceVertex, iDestVertex, null);
  }

  public ODocument createEdge(final ODocument iOutVertex, final ODocument iInVertex, final String iClassName) {
    return createEdge(iOutVertex, iInVertex, iClassName, (Object[]) null);
  }

  @SuppressWarnings("unchecked")
  public ODocument createEdge(final ODocument iOutVertex, final ODocument iInVertex, final String iClassName, Object... iFields) {
    if (iOutVertex == null)
      throw new IllegalArgumentException("iOutVertex is null");

    if (iInVertex == null)
      throw new IllegalArgumentException("iInVertex is null");

    final OClass cls = checkEdgeClass(iClassName);

    final boolean safeMode = beginBlock();
    try {

      final ODocument edge = new ODocument(cls).setOrdered(true);
      edge.field(EDGE_FIELD_OUT, iOutVertex);
      edge.field(EDGE_FIELD_IN, iInVertex);

      if (iFields != null)
        if (iFields.length == 1) {
          Object f = iFields[0];
          if (f instanceof Map<?, ?>)
            edge.fields((Map<String, Object>) f);
          else
            throw new IllegalArgumentException(
                "Invalid fields: expecting a pairs of fields as String,Object or a single Map<String,Object>, but found: " + f);
        } else
          // SET THE FIELDS
          for (int i = 0; i < iFields.length; i += 2)
            edge.field(iFields[i].toString(), iFields[i + 1]);

      // OUT FIELD
      updateVertexLinks(iOutVertex, edge, outV);

      // IN FIELD
      updateVertexLinks(iInVertex, edge, inV);

      edge.setDirty();

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

  private void updateVertexLinks(ODocument iVertex, ODocument edge, String vertexField) {
    acquireWriteLock(iVertex);
    try {

      final Object field = iVertex.field(vertexField);
      final Set<OIdentifiable> links;
      if (field instanceof OMVRBTreeRIDSet || field instanceof OSBTreeRIDSet) {
        links = (Set<OIdentifiable>) field;
      } else if (field instanceof Collection<?>) {
        if (preferSBTreeSet)
          links = new OSBTreeRIDSet(iVertex, (Collection<OIdentifiable>) field);
        else
          links = new OMVRBTreeRIDSet(iVertex, (Collection<OIdentifiable>) field);
        iVertex.field(vertexField, links);
      } else {
        links = createRIDSet(iVertex);
        iVertex.field(vertexField, links);
      }

      links.add(edge);
    } finally {
      releaseWriteLock(iVertex);
    }
  }

  @SuppressWarnings("unchecked")
  public boolean removeEdge(final OIdentifiable iEdge) {
    if (iEdge == null)
      return false;

    final ODocument edge = iEdge.getRecord();
    if (edge == null)
      return false;

    final boolean safeMode = beginBlock();
    try {
      // OUT VERTEX
      final ODocument outVertex = edge.field(EDGE_FIELD_OUT);

      acquireWriteLock(outVertex);
      try {

        if (outVertex != null) {
          final Set<OIdentifiable> out = getEdgeSet(outVertex, outV);
          if (out != null)
            out.remove(edge);
          save(outVertex);
        }

      } finally {
        releaseWriteLock(outVertex);
      }

      // IN VERTEX
      final ODocument inVertex = edge.field(EDGE_FIELD_IN);

      acquireWriteLock(inVertex);
      try {

        if (inVertex != null) {
          final Set<OIdentifiable> in = getEdgeSet(inVertex, inV);
          if (in != null)
            in.remove(edge);
          save(inVertex);
        }

      } finally {
        releaseWriteLock(inVertex);
      }

      delete(edge);

      commitBlock(safeMode);

    } catch (RuntimeException e) {
      rollbackBlock(safeMode);
      throw e;
    }
    return true;
  }

  public boolean removeVertex(final OIdentifiable iVertex) {
    if (iVertex == null)
      return false;

    final ODocument vertex = (ODocument) iVertex.getRecord();
    if (vertex == null)
      return false;

    final boolean safeMode = beginBlock();
    try {

      ODocument otherVertex;
      Set<OIdentifiable> otherEdges;

      // REMOVE OUT EDGES
      acquireWriteLock(vertex);
      try {

        Set<OIdentifiable> edges = getEdgeSet(vertex, outV);
        if (edges != null) {
          for (OIdentifiable e : edges) {
            if (e != null) {
              final ODocument edge = e.getRecord();
              if (edge != null) {
                otherVertex = edge.field(EDGE_FIELD_IN);
                if (otherVertex != null) {
                  otherEdges = getEdgeSet(otherVertex, inV);
                  if (otherEdges != null && otherEdges.remove(edge))
                    save(otherVertex);
                }
                delete(edge);
              }
            }
          }
        }

        // REMOVE IN EDGES
        edges = getEdgeSet(vertex, inV);
        if (edges != null) {
          for (OIdentifiable e : edges) {
            if (e != null) {
              if (e != null) {
                final ODocument edge = e.getRecord();
                otherVertex = edge.field(EDGE_FIELD_OUT);
                if (otherVertex != null) {
                  otherEdges = getEdgeSet(otherVertex, outV);
                  if (otherEdges != null && otherEdges.remove(edge))
                    save(otherVertex);
                }
                delete(edge);
              }
            }
          }
        }

        // DELETE VERTEX AS DOCUMENT
        delete(vertex);

      } finally {
        releaseWriteLock(vertex);
      }

      commitBlock(safeMode);

      return true;

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
  public Set<OIdentifiable> getEdgesBetweenVertexes(final OIdentifiable iVertex1, final OIdentifiable iVertex2) {
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
  public Set<OIdentifiable> getEdgesBetweenVertexes(final OIdentifiable iVertex1, final OIdentifiable iVertex2,
      final String[] iLabels) {
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
  public Set<OIdentifiable> getEdgesBetweenVertexes(final OIdentifiable iVertex1, final OIdentifiable iVertex2,
      final String[] iLabels, final String[] iClassNames) {
    final Set<OIdentifiable> result = new HashSet<OIdentifiable>();

    if (iVertex1 != null && iVertex2 != null) {
      acquireReadLock(iVertex1);
      try {

        // CHECK OUT EDGES
        for (OIdentifiable e : getOutEdges(iVertex1)) {
          final ODocument edge = (ODocument) e.getRecord();

          if (checkEdge(edge, iLabels, iClassNames)) {
            final OIdentifiable in = edge.<ODocument> field("in");
            if (in != null && in.equals(iVertex2))
              result.add(edge);
          }
        }

        // CHECK IN EDGES
        for (OIdentifiable e : getInEdges(iVertex1)) {
          final ODocument edge = (ODocument) e.getRecord();

          if (checkEdge(edge, iLabels, iClassNames)) {
            final OIdentifiable out = edge.<ODocument> field("out");
            if (out != null && out.equals(iVertex2))
              result.add(edge);
          }
        }

      } finally {
        releaseReadLock(iVertex1);
      }

    }

    return result;
  }

  public Set<OIdentifiable> getOutEdges(final OIdentifiable iVertex) {
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
  public Set<OIdentifiable> getOutEdges(final OIdentifiable iVertex, final String iLabel) {
    if (iVertex == null)
      return null;

    final ODocument vertex = iVertex.getRecord();
    checkVertexClass(vertex);

    Set<OIdentifiable> result = null;

    acquireReadLock(iVertex);
    try {

      final Set<OIdentifiable> set = getEdgeSet(vertex, outV);

      if (iLabel == null)
        // RETURN THE ENTIRE COLLECTION
        if (set != null)
          return Collections.unmodifiableSet(set);
        else
          return Collections.emptySet();

      // FILTER BY LABEL
      result = new HashSet<OIdentifiable>();
      if (set != null)
        for (OIdentifiable item : set) {
          if (iLabel == null || iLabel.equals(((ODocument) item).field(LABEL)))
            result.add(item);
        }

    } finally {
      releaseReadLock(iVertex);
    }

    return result;
  }

  @SuppressWarnings("unchecked")
  protected Set<OIdentifiable> getEdgeSet(final ODocument iVertex, final String iFieldName) {
    final Object value = iVertex.field(iFieldName);
    if (value != null && (value instanceof OMVRBTreeRIDSet || value instanceof OSBTreeRIDSet))
      return (Set<OIdentifiable>) value;

    final Set<OIdentifiable> set = createRIDSet(iVertex);

    if (OMultiValue.isMultiValue(value))
      // AUTOCONVERT FROM COLLECTION
      set.addAll((Collection<? extends OIdentifiable>) value);
    else
      // AUTOCONVERT FROM SINGLE VALUE
      set.add((OIdentifiable) value);
    return set;
  }

  private Set<OIdentifiable> createRIDSet(ODocument iVertex) {
    if (preferSBTreeSet)
      return new OSBTreeRIDSet(iVertex);
    else
      return new OMVRBTreeRIDSet(iVertex);
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
  public Set<OIdentifiable> getOutEdgesHavingProperties(final OIdentifiable iVertex, final Map<String, Object> iProperties) {
    if (iVertex == null)
      return null;

    final ODocument vertex = iVertex.getRecord();
    checkVertexClass(vertex);

    return filterEdgesByProperties(getEdgeSet(vertex, outV), iProperties);
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
  public Set<OIdentifiable> getOutEdgesHavingProperties(final OIdentifiable iVertex, Iterable<String> iProperties) {
    if (iVertex == null)
      return null;

    final ODocument vertex = iVertex.getRecord();
    checkVertexClass(vertex);

    return filterEdgesByProperties(getEdgeSet(vertex, outV), iProperties);
  }

  public Set<OIdentifiable> getInEdges(final OIdentifiable iVertex) {
    return getInEdges(iVertex, null);
  }

  public Set<OIdentifiable> getInEdges(final OIdentifiable iVertex, final String iLabel) {
    if (iVertex == null)
      return null;

    final ODocument vertex = iVertex.getRecord();
    checkVertexClass(vertex);

    Set<OIdentifiable> result = null;

    acquireReadLock(iVertex);
    try {

      final Set<OIdentifiable> set = getEdgeSet(vertex, inV);

      if (iLabel == null)
        // RETURN THE ENTIRE COLLECTION
        if (set != null)
          return Collections.unmodifiableSet(set);
        else
          return Collections.emptySet();

      // FILTER BY LABEL
      result = new HashSet<OIdentifiable>();
      if (set != null)
        for (OIdentifiable item : set) {
          if (iLabel == null || iLabel.equals(((ODocument) item).field(LABEL)))
            result.add(item);
        }

    } finally {
      releaseReadLock(iVertex);
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
  public Set<OIdentifiable> getInEdgesHavingProperties(final OIdentifiable iVertex, Iterable<String> iProperties) {
    if (iVertex == null)
      return null;

    final ODocument vertex = iVertex.getRecord();
    checkVertexClass(vertex);

    return filterEdgesByProperties(getEdgeSet(vertex, inV), iProperties);
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
    if (iVertex == null)
      return null;

    checkVertexClass(iVertex);
    return filterEdgesByProperties(getEdgeSet(iVertex, inV), iProperties);
  }

  public ODocument getInVertex(final OIdentifiable iEdge) {
    if (iEdge == null)
      return null;

    final ODocument e = (ODocument) iEdge.getRecord();

    checkEdgeClass(e);
    OIdentifiable v = e.field(EDGE_FIELD_IN);
    if (v != null && v instanceof ORID) {
      // REPLACE WITH THE DOCUMENT
      v = v.getRecord();
      final boolean wasDirty = e.isDirty();
      e.field(EDGE_FIELD_IN, v);
      if (!wasDirty)
        e.unsetDirty();
    }

    return (ODocument) v;
  }

  public ODocument getOutVertex(final OIdentifiable iEdge) {
    if (iEdge == null)
      return null;

    final ODocument e = (ODocument) iEdge.getRecord();

    checkEdgeClass(e);
    OIdentifiable v = e.field(EDGE_FIELD_OUT);
    if (v != null && v instanceof ORID) {
      // REPLACE WITH THE DOCUMENT
      v = v.getRecord();
      final boolean wasDirty = e.isDirty();
      e.field(EDGE_FIELD_OUT, v);
      if (!wasDirty)
        e.unsetDirty();
    }

    return (ODocument) v;
  }

  public Set<OIdentifiable> filterEdgesByProperties(final Set<OIdentifiable> iEdges, final Iterable<String> iPropertyNames) {
    acquireReadLock(null);
    try {

      if (iPropertyNames == null)
        // RETURN THE ENTIRE COLLECTION
        if (iEdges != null)
          return Collections.unmodifiableSet(iEdges);
        else
          return Collections.emptySet();

      // FILTER BY PROPERTY VALUES
      final Set<OIdentifiable> result = new HashSet<OIdentifiable>();
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

    } finally {
      releaseReadLock(null);
    }
  }

  public Set<OIdentifiable> filterEdgesByProperties(final Set<OIdentifiable> iEdges, final Map<String, Object> iProperties) {
    acquireReadLock(null);
    try {

      if (iProperties == null)
        // RETURN THE ENTIRE COLLECTION
        if (iEdges != null)
          return Collections.unmodifiableSet(iEdges);
        else
          return Collections.emptySet();

      // FILTER BY PROPERTY VALUES
      final OMVRBTreeRIDSet result;
      result = new OMVRBTreeRIDSet();
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
    } finally {
      releaseReadLock(null);
    }
  }

  public ODocument getRoot(final String iName) {
    return getDictionary().get(iName);
  }

  public ODocument getRoot(final String iName, final String iFetchPlan) {
    return getDictionary().get(iName, iFetchPlan);
  }

  public OGraphDatabase setRoot(final String iName, final ODocument iNode) {
    if (iNode == null)
      getDictionary().remove(iName);
    else
      getDictionary().put(iName, iNode);
    return this;
  }

  public OClass createVertexType(final String iClassName) {
    return getMetadata().getSchema().createClass(iClassName, vertexBaseClass);
  }

  public OClass createVertexType(final String iClassName, final String iSuperClassName) {
    return getMetadata().getSchema().createClass(iClassName, checkVertexClass(iSuperClassName));
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
    return getMetadata().getSchema().createClass(iClassName, checkEdgeClass(iSuperClassName));
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

  public void checkVertexClass(final ODocument iVertex) {
    // FORCE EARLY UNMARSHALLING
    iVertex.deserializeFields();

    if (useCustomTypes && !iVertex.getSchemaClass().isSubClassOf(vertexBaseClass))
      throw new IllegalArgumentException("The document received is not a vertex. Found class '" + iVertex.getSchemaClass() + "'");
  }

  public OClass checkVertexClass(final String iVertexTypeName) {
    if (iVertexTypeName == null || !useCustomTypes)
      return getVertexBaseClass();

    final OClass cls = getMetadata().getSchema().getClass(iVertexTypeName);
    if (cls == null)
      throw new IllegalArgumentException("The class '" + iVertexTypeName + "' was not found");

    if (!cls.isSubClassOf(vertexBaseClass))
      throw new IllegalArgumentException("The class '" + iVertexTypeName + "' does not extend the vertex type");

    return cls;
  }

  public void checkVertexClass(final OClass iVertexType) {
    if (useCustomTypes && iVertexType != null) {
      if (!iVertexType.isSubClassOf(vertexBaseClass))
        throw new IllegalArgumentException("The class '" + iVertexType + "' does not extend the vertex type");
    }
  }

  public void checkEdgeClass(final ODocument iEdge) {
    // FORCE EARLY UNMARSHALLING
    iEdge.deserializeFields();

    if (useCustomTypes && !iEdge.getSchemaClass().isSubClassOf(edgeBaseClass))
      throw new IllegalArgumentException("The document received is not an edge. Found class '" + iEdge.getSchemaClass() + "'");
  }

  public OClass checkEdgeClass(final String iEdgeTypeName) {
    if (iEdgeTypeName == null || !useCustomTypes)
      return getEdgeBaseClass();

    final OClass cls = getMetadata().getSchema().getClass(iEdgeTypeName);
    if (cls == null)
      throw new IllegalArgumentException("The class '" + iEdgeTypeName + "' was not found");

    if (!cls.isSubClassOf(edgeBaseClass))
      throw new IllegalArgumentException("The class '" + iEdgeTypeName + "' does not extend the edge type");

    return cls;
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

  /**
   * Locks the record in exclusive mode to avoid concurrent access.
   * 
   * @param iRecord
   *          Record to lock
   * @return The current instance as fluent interface to allow calls in chain.
   */
  public OGraphDatabase acquireWriteLock(final OIdentifiable iRecord) {
    switch (lockMode) {
    case DATABASE_LEVEL_LOCKING:
      ((OStorage) getStorage()).getLock().acquireExclusiveLock();
      break;
    case RECORD_LEVEL_LOCKING:
      ((OStorageEmbedded) getStorage()).acquireWriteLock(iRecord.getIdentity());
      break;
    case NO_LOCKING:
      break;
    }
    return this;
  }

  /**
   * Releases the exclusive lock against a record previously acquired by current thread.
   * 
   * @param iRecord
   *          Record to unlock
   * @return The current instance as fluent interface to allow calls in chain.
   */
  public OGraphDatabase releaseWriteLock(final OIdentifiable iRecord) {
    switch (lockMode) {
    case DATABASE_LEVEL_LOCKING:
      ((OStorage) getStorage()).getLock().releaseExclusiveLock();
      break;
    case RECORD_LEVEL_LOCKING:
      ((OStorageEmbedded) getStorage()).releaseWriteLock(iRecord.getIdentity());
      break;
    case NO_LOCKING:
      break;
    }
    return this;
  }

  /**
   * Locks the record in shared mode to avoid concurrent writes.
   * 
   * @param iRecord
   *          Record to lock
   * @return The current instance as fluent interface to allow calls in chain.
   */
  public OGraphDatabase acquireReadLock(final OIdentifiable iRecord) {
    switch (lockMode) {
    case DATABASE_LEVEL_LOCKING:
      ((OStorage) getStorage()).getLock().acquireSharedLock();
      break;
    case RECORD_LEVEL_LOCKING:
      ((OStorageEmbedded) getStorage()).acquireReadLock(iRecord.getIdentity());
      break;
    case NO_LOCKING:
      break;
    }
    return this;
  }

  /**
   * Releases the shared lock against a record previously acquired by current thread.
   * 
   * @param iRecord
   *          Record to unlock
   * @return The current instance as fluent interface to allow calls in chain.
   */
  public OGraphDatabase releaseReadLock(final OIdentifiable iRecord) {
    switch (lockMode) {
    case DATABASE_LEVEL_LOCKING:
      ((OStorage) getStorage()).getLock().releaseSharedLock();
      break;
    case RECORD_LEVEL_LOCKING:
      ((OStorageEmbedded) getStorage()).releaseReadLock(iRecord.getIdentity());
      break;
    case NO_LOCKING:
      break;
    }
    return this;
  }

  @Override
  public String getType() {
    return TYPE;
  }

  public void checkForGraphSchema() {
    getMetadata().getSchema().getOrCreateClass(OMVRBTreeRIDProvider.PERSISTENT_CLASS_NAME);

    vertexBaseClass = getMetadata().getSchema().getClass(VERTEX_ALIAS);
    edgeBaseClass = getMetadata().getSchema().getClass(EDGE_ALIAS);

    if (vertexBaseClass == null) {
      // CREATE THE META MODEL USING THE ORIENT SCHEMA
      vertexBaseClass = getMetadata().getSchema().createClass(VERTEX_ALIAS);
      vertexBaseClass.setOverSize(2);
    }

    if (edgeBaseClass == null) {
      edgeBaseClass = getMetadata().getSchema().createClass(EDGE_ALIAS);
      edgeBaseClass.setShortName(EDGE_ALIAS);
    }
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

  public LOCK_MODE getLockMode() {
    return lockMode;
  }

  public void setLockMode(final LOCK_MODE lockMode) {
    if (lockMode == LOCK_MODE.RECORD_LEVEL_LOCKING && !(getStorage() instanceof OStorageEmbedded))
      // NOT YET SUPPORETD REMOTE LOCKING
      throw new IllegalArgumentException("Record leve locking is not supported for remote connections");

    this.lockMode = lockMode;
  }

  public boolean isRetroCompatibility() {
    return retroCompatibility;
  }

  public void setRetroCompatibility(final boolean retroCompatibility) {
    this.retroCompatibility = retroCompatibility;
    if (retroCompatibility) {
      inV = VERTEX_FIELD_IN_OLD;
      outV = VERTEX_FIELD_OUT_OLD;
    } else {
      inV = VERTEX_FIELD_IN;
      outV = VERTEX_FIELD_OUT;
    }
  }
}
