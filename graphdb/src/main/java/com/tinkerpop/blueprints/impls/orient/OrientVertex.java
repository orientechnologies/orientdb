/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */

package com.tinkerpop.blueprints.impls.orient;

import com.orientechnologies.common.collection.OMultiCollectionIterator;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.common.util.OTriple;
import com.orientechnologies.orient.core.command.OCommandPredicate;
import com.orientechnologies.orient.core.command.traverse.OTraverse;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OAutoConvertToRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordLazyList;
import com.orientechnologies.orient.core.db.record.ORecordLazyMultiValue;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Index;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.ExceptionFactory;
import com.tinkerpop.blueprints.util.StringFactory;
import com.tinkerpop.blueprints.util.wrappers.partition.PartitionVertex;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * OrientDB Vertex implementation of TinkerPop Blueprints standard.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com) (http://orientdb.com)
 */
@SuppressWarnings("unchecked")
public class OrientVertex extends OrientElement implements OrientExtendedVertex {
  public static final String CONNECTION_OUT_PREFIX = OrientBaseGraph.CONNECTION_OUT + "_";
  public static final String CONNECTION_IN_PREFIX = OrientBaseGraph.CONNECTION_IN + "_";

  private static final long serialVersionUID = 1L;

  /** (Internal) Called by serialization. */
  public OrientVertex() {
    super(null, null);
  }

  protected OrientVertex(final OrientBaseGraph graph, String className, final Object... fields) {
    super(graph, null);
    if (className != null)
      className = checkForClassInSchema(OrientBaseGraph.encodeClassName(className));

    // TODO: should it rather be an OVertexDocument?
    rawElement = new ODocument(className == null ? OrientVertexType.CLASS_NAME : className);
    setPropertiesInternal(fields);
  }

  public OrientVertex(final OrientBaseGraph graph, final OIdentifiable record) {
    super(graph, record);
  }

  /**
   * (Internal only) Returns the field name used for the relationship.
   *
   * @param iDirection Direction between IN, OUT or BOTH
   * @param iClassName Class name if any
   * @param useVertexFieldsForEdgeLabels Graph setting about using the edge label as vertex's field
   */
  public static String getConnectionFieldName(
      final Direction iDirection,
      final String iClassName,
      final boolean useVertexFieldsForEdgeLabels) {
    if (iDirection == null || iDirection == Direction.BOTH)
      throw new IllegalArgumentException("Direction not valid");

    if (useVertexFieldsForEdgeLabels) {
      // PREFIX "out_" or "in_" TO THE FIELD NAME
      final String prefix =
          iDirection == Direction.OUT ? CONNECTION_OUT_PREFIX : CONNECTION_IN_PREFIX;
      if (iClassName == null
          || iClassName.isEmpty()
          || iClassName.equals(OrientEdgeType.CLASS_NAME)) return prefix;

      return prefix + iClassName;
    } else
      // "out" or "in"
      return iDirection == Direction.OUT
          ? OrientBaseGraph.CONNECTION_OUT
          : OrientBaseGraph.CONNECTION_IN;
  }

  /** (Internal only) Creates a link between a vertices and a Graph Element. */
  public static Object createLink(
      final OrientBaseGraph iGraph,
      final ODocument iFromVertex,
      final OIdentifiable iTo,
      final String iFieldName) {
    final Object out;
    OType outType = iFromVertex.fieldType(iFieldName);
    Object found = iFromVertex.field(iFieldName);

    final OClass linkClass = ODocumentInternal.getImmutableSchemaClass(iFromVertex);
    if (linkClass == null)
      throw new IllegalArgumentException("Class not found in source vertex: " + iFromVertex);

    final OProperty prop = linkClass.getProperty(iFieldName);
    final OType propType = prop != null && prop.getType() != OType.ANY ? prop.getType() : null;

    if (found == null) {
      if (iGraph.isAutoScaleEdgeType()
          && (prop == null
              || propType == OType.LINK
              || "true"
                  .equalsIgnoreCase(
                      prop.getCustom(OrientVertexType.OrientVertexProperty.ORDERED)))) {
        // CREATE ONLY ONE LINK
        out = iTo;
        outType = OType.LINK;
      } else if (propType == OType.LINKLIST
          || (prop != null
              && "true"
                  .equalsIgnoreCase(
                      prop.getCustom(OrientVertexType.OrientVertexProperty.ORDERED)))) {
        final Collection coll = new ORecordLazyList(iFromVertex);
        coll.add(iTo);
        out = coll;
        outType = OType.LINKLIST;
      } else if (propType == null || propType == OType.LINKBAG) {
        final ORidBag bag = new ORidBag();
        bag.add(iTo);
        out = bag;
        outType = OType.LINKBAG;
      } else
        throw new IllegalStateException(
            "Type of field provided in schema '"
                + prop.getType()
                + "' cannot be used for link creation.");

    } else if (found instanceof OIdentifiable) {
      if (prop != null && propType == OType.LINK)
        throw new IllegalStateException(
            "Type of field provided in schema '"
                + prop.getType()
                + "' cannot be used for creation to hold several links.");

      if (prop != null
          && "true"
              .equalsIgnoreCase(prop.getCustom(OrientVertexType.OrientVertexProperty.ORDERED))) {
        final Collection coll = new ORecordLazyList(iFromVertex);
        coll.add(found);
        coll.add(iTo);
        out = coll;
        outType = OType.LINKLIST;
      } else {
        final ORidBag bag = new ORidBag();
        bag.add((OIdentifiable) found);
        bag.add(iTo);
        out = bag;
        outType = OType.LINKBAG;
      }
    } else if (found instanceof ORidBag) {
      // ADD THE LINK TO THE COLLECTION
      out = null;

      ((ORidBag) found).add(iTo);

    } else if (found instanceof Collection<?>) {
      // USE THE FOUND COLLECTION
      out = null;
      ((Collection<Object>) found).add(iTo);

    } else
      throw new IllegalStateException(
          "Relationship content is invalid on field " + iFieldName + ". Found: " + found);

    if (out != null)
      // OVERWRITE IT
      iFromVertex.field(iFieldName, out, outType);

    return out;
  }

  public static Direction getConnectionDirection(
      final String iConnectionField, final boolean useVertexFieldsForEdgeLabels) {
    if (iConnectionField == null)
      throw new IllegalArgumentException("Cannot return direction of NULL connection ");

    if (useVertexFieldsForEdgeLabels) {
      if (iConnectionField.startsWith(CONNECTION_OUT_PREFIX)) return Direction.OUT;
      else if (iConnectionField.startsWith(CONNECTION_IN_PREFIX)) return Direction.IN;
    } else {
      if (iConnectionField.equals(OrientBaseGraph.CONNECTION_OUT)) return Direction.OUT;
      else if (iConnectionField.startsWith(OrientBaseGraph.CONNECTION_IN)) return Direction.IN;
    }

    throw new IllegalArgumentException("Cannot return direction of connection " + iConnectionField);
  }

  /** (Internal only) */
  public static String getInverseConnectionFieldName(
      final String iFieldName, final boolean useVertexFieldsForEdgeLabels) {
    if (useVertexFieldsForEdgeLabels) {
      if (iFieldName.startsWith(CONNECTION_OUT_PREFIX)) {
        if (iFieldName.length() == CONNECTION_OUT_PREFIX.length())
          // "OUT" CASE
          return CONNECTION_IN_PREFIX;

        return CONNECTION_IN_PREFIX + iFieldName.substring(CONNECTION_OUT_PREFIX.length());

      } else if (iFieldName.startsWith(CONNECTION_IN_PREFIX)) {
        if (iFieldName.length() == CONNECTION_IN_PREFIX.length())
          // "IN" CASE
          return CONNECTION_OUT_PREFIX;

        return CONNECTION_OUT_PREFIX + iFieldName.substring(CONNECTION_IN_PREFIX.length());

      } else
        throw new IllegalArgumentException(
            "Cannot find reverse connection name for field " + iFieldName);
    }

    if (iFieldName.equals(OrientBaseGraph.CONNECTION_OUT)) return OrientBaseGraph.CONNECTION_IN;
    else if (iFieldName.equals(OrientBaseGraph.CONNECTION_IN))
      return OrientBaseGraph.CONNECTION_OUT;

    throw new IllegalArgumentException(
        "Cannot find reverse connection name for field " + iFieldName);
  }

  /** (Internal only) */
  public static void replaceLinks(
      final ODocument iVertex,
      final String iFieldName,
      final OIdentifiable iVertexToRemove,
      final OIdentifiable iNewVertex) {
    if (iVertex == null) return;

    final Object fieldValue =
        iVertexToRemove != null ? iVertex.field(iFieldName) : iVertex.removeField(iFieldName);
    if (fieldValue == null) return;

    if (fieldValue instanceof OIdentifiable) {
      // SINGLE RECORD

      if (iVertexToRemove != null) {
        if (!fieldValue.equals(iVertexToRemove)) {
          return;
        }
        iVertex.field(iFieldName, iNewVertex);
      }

    } else if (fieldValue instanceof ORidBag) {
      // COLLECTION OF RECORDS: REMOVE THE ENTRY
      final ORidBag bag = (ORidBag) fieldValue;

      boolean found = false;
      final Iterator<OIdentifiable> it = bag.rawIterator();
      while (it.hasNext()) {
        if (it.next().equals(iVertexToRemove)) {
          // REMOVE THE OLD ENTRY
          found = true;
          it.remove();
        }
      }
      if (found)
        // ADD THE NEW ONE
        bag.add(iNewVertex);

    } else if (fieldValue instanceof Collection) {
      final Collection col = (Collection) fieldValue;

      if (col.remove(iVertexToRemove)) col.add(iNewVertex);
    }

    iVertex.save();
  }

  /** (Internal only) */
  protected static OrientEdge getEdge(
      final OrientBaseGraph graph,
      final ODocument doc,
      String fieldName,
      final OPair<Direction, String> connection,
      final Object fieldValue,
      final OIdentifiable iTargetVertex,
      final String[] iLabels) {
    final OrientEdge toAdd;

    final ODocument fieldRecord = ((OIdentifiable) fieldValue).getRecord();
    if (fieldRecord == null) return null;

    OClass klass = ODocumentInternal.getImmutableSchemaClass(fieldRecord);
    if (klass == null && ODatabaseRecordThreadLocal.instance().getIfDefined() != null) {
      ODatabaseRecordThreadLocal.instance().getIfDefined().getMetadata().reload();
      klass = fieldRecord.getSchemaClass();
    }

    if (klass.isVertexType()) {
      if (iTargetVertex != null && !iTargetVertex.equals(fieldValue)) return null;

      // DIRECT VERTEX, CREATE A DUMMY EDGE BETWEEN VERTICES
      if (connection.getKey() == Direction.OUT)
        toAdd = graph.getEdgeInstance(doc, fieldRecord, connection.getValue());
      else toAdd = graph.getEdgeInstance(fieldRecord, doc, connection.getValue());

    } else if (klass.isEdgeType()) {
      // EDGE
      if (iTargetVertex != null) {
        Object targetVertex = OrientEdge.getConnection(fieldRecord, connection.getKey().opposite());

        if (!iTargetVertex.equals(targetVertex)) return null;
      }

      toAdd = graph.getEdge(fieldRecord);
    } else
      throw new IllegalStateException(
          "Invalid content found in " + fieldName + " field: " + fieldRecord);

    return toAdd;
  }

  public OrientVertex copy() {
    final OrientVertex v = new OrientVertex();
    super.copyTo(v);
    return v;
  }

  @Override
  public OrientVertex getVertexInstance() {
    return this;
  }

  /**
   * (Blueprints Extension) Executes the command predicate against current vertex. Use OSQLPredicate
   * to execute SQL. Example: <code>
   * Iterable<OrientVertex> friendsOfFriends = (Iterable<OrientVertex>) luca.execute(new OSQLPredicate("out().out('Friend').out('Friend')"));
   * </code>
   *
   * @param iPredicate Predicate to evaluate. Use OSQLPredicate to use SQL
   */
  public Object execute(final OCommandPredicate iPredicate) {
    final Object result = iPredicate.evaluate(rawElement.getRecord(), null, null);

    if (result instanceof OAutoConvertToRecord)
      ((OAutoConvertToRecord) result).setAutoConvertToRecord(true);

    return result;
  }

  /**
   * Returns all the Property names as Set of String. out, in and label are not returned as
   * properties even if are part of the underlying document because are considered internal
   * properties.
   */
  @Override
  public Set<String> getPropertyKeys() {
    final OrientBaseGraph graph = setCurrentGraphInThreadLocal();

    final ODocument doc = getRecord();

    final Set<String> result = new HashSet<String>();

    for (String field : doc.fieldNames())
      if (graph != null && settings.isUseVertexFieldsForEdgeLabels()) {
        if (!field.startsWith(CONNECTION_OUT_PREFIX) && !field.startsWith(CONNECTION_IN_PREFIX))
          result.add(field);
      } else if (!field.equals(OrientBaseGraph.CONNECTION_OUT)
          && !field.equals(OrientBaseGraph.CONNECTION_IN)) result.add(field);

    return result;
  }

  @Override
  public Map<String, Object> getProperties() {
    if (this.rawElement == null) return null;
    final ODocument raw = this.rawElement.getRecord();
    if (raw == null) return null;

    final OrientBaseGraph graph = setCurrentGraphInThreadLocal();

    final Map<String, Object> result = new HashMap<String, Object>();

    for (String field : raw.fieldNames())
      if (graph != null && settings.isUseVertexFieldsForEdgeLabels()) {
        if (!field.startsWith(CONNECTION_OUT_PREFIX) && !field.startsWith(CONNECTION_IN_PREFIX))
          result.put(field, raw.field(field));
      } else if (!field.equals(OrientBaseGraph.CONNECTION_OUT)
          && !field.equals(OrientBaseGraph.CONNECTION_IN)) result.put(field, raw.field(field));

    return result;
  }

  /**
   * Returns a lazy iterable instance against vertices.
   *
   * @param iDirection The direction between OUT, IN or BOTH
   * @param iLabels Optional varargs of Strings representing edge label to consider
   */
  @Override
  public Iterable<Vertex> getVertices(final Direction iDirection, final String... iLabels) {
    setCurrentGraphInThreadLocal();

    OrientBaseGraph.getEdgeClassNames(getGraph(), iLabels);
    OrientBaseGraph.encodeClassNames(iLabels);

    final ODocument doc = getRecord();

    final OMultiCollectionIterator<Vertex> iterable = new OMultiCollectionIterator<Vertex>();

    for (OTriple<String, Direction, String> connectionField :
        getConnectionFields(iDirection, iLabels)) {
      String fieldName = connectionField.getKey();
      OPair<Direction, String> connection = connectionField.getValue();

      final Object fieldValue = doc.rawField(fieldName);
      if (fieldValue != null)
        if (fieldValue instanceof OIdentifiable) {
          addSingleVertex(doc, iterable, fieldName, connection, fieldValue, iLabels);

        } else if (fieldValue instanceof Collection<?>) {
          Collection<?> coll = (Collection<?>) fieldValue;

          if (coll.size() == 1) {
            // SINGLE ITEM: AVOID CALLING ITERATOR
            if (coll instanceof ORecordLazyMultiValue)
              addSingleVertex(
                  doc,
                  iterable,
                  fieldName,
                  connection,
                  ((ORecordLazyMultiValue) coll).rawIterator().next(),
                  iLabels);
            else if (coll instanceof List<?>)
              addSingleVertex(
                  doc, iterable, fieldName, connection, ((List<?>) coll).get(0), iLabels);
            else
              addSingleVertex(
                  doc, iterable, fieldName, connection, coll.iterator().next(), iLabels);
          } else {
            // CREATE LAZY Iterable AGAINST COLLECTION FIELD
            if (coll instanceof ORecordLazyMultiValue)
              iterable.add(
                  new OrientVertexIterator(
                      this,
                      coll,
                      ((ORecordLazyMultiValue) coll).rawIterator(),
                      connection,
                      iLabels,
                      coll.size()));
            else
              iterable.add(
                  new OrientVertexIterator(this, coll, coll.iterator(), connection, iLabels, -1));
          }
        } else if (fieldValue instanceof ORidBag) {
          iterable.add(
              new OrientVertexIterator(
                  this, fieldValue, ((ORidBag) fieldValue).rawIterator(), connection, iLabels, -1));
        }
    }

    return iterable;
  }

  /** Executes a query against the current vertex. The returning type is a OrientVertexQuery. */
  @Override
  public OrientVertexQuery query() {
    setCurrentGraphInThreadLocal();
    return new OrientVertexQuery(this);
  }

  /** Returns a OTraverse object to start traversing from the current vertex. */
  public OTraverse traverse() {
    setCurrentGraphInThreadLocal();
    return new OTraverse().target(getRecord());
  }

  /**
   * Removes the current Vertex from the Graph. all the incoming and outgoing edges are
   * automatically removed too.
   */
  @Override
  public void remove() {
    checkClass();

    final OrientBaseGraph graph = checkIfAttached();

    graph.setCurrentGraphInThreadLocal();
    graph.autoStartTransaction();

    final ODocument doc = getRecord();
    if (doc == null) throw ExceptionFactory.vertexWithIdDoesNotExist(this.getId());

    Map<String, List<ODocument>> treeRidbagEdgesToRemove = new HashMap<String, List<ODocument>>();

    if (!graph.getRawGraph().getTransaction().isActive()) {
      for (String fieldName : doc.fieldNames()) {
        final OPair<Direction, String> connection = getConnection(Direction.BOTH, fieldName);
        if (connection == null)
          // SKIP THIS FIELD
          continue;
        Object fv = doc.field(fieldName);
        if (fv instanceof ORidBag && !((ORidBag) fv).isEmbedded()) {
          List<ODocument> docs = new ArrayList<ODocument>();
          for (OIdentifiable id : (ORidBag) fv) docs.add(OrientBaseGraph.getDocument(id, true));
          treeRidbagEdgesToRemove.put(fieldName, docs);
        }
      }
    }

    // REMOVE THE VERTEX RECORD FIRST TO CATCH CME BEFORE EDGES ARE REMOVED
    super.removeRecord();

    // REMOVE THE VERTEX FROM MANUAL INDEXES
    final Iterator<Index<? extends Element>> it = graph.getIndices().iterator();
    if (it.hasNext()) {
      final Set<Edge> allEdges = new HashSet<Edge>();
      for (Edge e : getEdges(Direction.BOTH)) allEdges.add(e);

      while (it.hasNext()) {
        final Index<? extends Element> index = it.next();

        if (Vertex.class.isAssignableFrom(index.getIndexClass())) {
          final OrientIndex<OrientVertex> idx = (OrientIndex<OrientVertex>) index;
          idx.removeElement(this);
        }

        if (Edge.class.isAssignableFrom(index.getIndexClass())) {
          final OrientIndex<OrientEdge> idx = (OrientIndex<OrientEdge>) index;
          for (final Edge e : allEdges) idx.removeElement((OrientEdge) e);
        }
      }
    }

    for (Map.Entry<String, List<ODocument>> entry : treeRidbagEdgesToRemove.entrySet()) {
      doc.removeField(entry.getKey());
      Iterator<ODocument> iter = entry.getValue().iterator();
      while (iter.hasNext()) {
        ODocument docEdge = iter.next();
        OrientBaseGraph.deleteEdgeIfAny(docEdge, false);
      }
    }

    graph.removeEdgesInternal(
        this,
        doc,
        null,
        true,
        settings.isUseVertexFieldsForEdgeLabels(),
        settings.isAutoScaleEdgeType());
  }

  /**
   * Moves current vertex to another class. All edges are updated automatically.
   *
   * @param iClassName New class name to assign
   * @return New vertex's identity
   * @see #moveToCluster(String)
   * @see #moveTo(String, String)
   */
  public ORID moveToClass(final String iClassName) {
    return moveTo(iClassName, null);
  }

  /**
   * Moves current vertex to another cluster. All edges are updated automatically.
   *
   * @param iClusterName Cluster name where to save the new vertex
   * @return New vertex's identity
   * @see #moveToClass(String)
   * @see #moveTo(String, String)
   */
  public ORID moveToCluster(final String iClusterName) {
    return moveTo(null, iClusterName);
  }

  /**
   * Moves current vertex to another class/cluster. All edges are updated automatically.
   *
   * @param iClassName New class name to assign
   * @param iClusterName Cluster name where to save the new vertex
   * @return New vertex's identity
   * @see #moveToClass(String)
   * @see #moveToCluster(String)
   */
  public ORID moveTo(final String iClassName, final String iClusterName) {
    return getRecord().asVertex().get().moveTo(iClassName, iClusterName);
  }

  /**
   * Creates an edge between current Vertex and a target Vertex setting label as Edge's label.
   *
   * @param label Edge's label or class
   * @param inVertex Outgoing target vertex
   * @return The new Edge created
   */
  @Override
  public Edge addEdge(final String label, Vertex inVertex) {
    if (inVertex instanceof PartitionVertex)
      // WRAPPED: GET THE BASE VERTEX
      inVertex = ((PartitionVertex) inVertex).getBaseVertex();

    return addEdge(label, (OrientVertex) inVertex, null, null, (Object[]) null);
  }

  /**
   * Creates an edge between current Vertex and a target Vertex setting label as Edge's label.
   * iClassName is the Edge's class used if different by label.
   *
   * @param label Edge's label or class
   * @param inVertex Outgoing target vertex
   * @param iClassName Edge's class name
   * @return The new Edge created
   */
  public OrientEdge addEdge(
      final String label, final OrientVertex inVertex, final String iClassName) {
    return addEdge(label, inVertex, iClassName, null, (Object[]) null);
  }

  /**
   * Creates an edge between current Vertex and a target Vertex setting label as Edge's label. The
   * fields parameter is an Array of fields to set on Edge upon creation. Fields must be a odd pairs
   * of key/value or a single object as Map containing entries as key/value pairs.
   *
   * @param label Edge's label or class
   * @param inVertex Outgoing target vertex
   * @param fields Fields must be a odd pairs of key/value or a single object as Map containing
   *     entries as key/value pairs
   * @return The new Edge created
   */
  public OrientEdge addEdge(
      final String label, final OrientVertex inVertex, final Object[] fields) {
    return addEdge(label, inVertex, null, null, fields);
  }

  /**
   * Creates an edge between current Vertex and a target Vertex setting label as Edge's label. The
   * fields parameter is an Array of fields to set on Edge upon creation. Fields must be a odd pairs
   * of key/value or a single object as Map containing entries as key/value pairs. iClusterName is
   * the name of the cluster where to store the new Edge.
   *
   * @param label Edge's label or class
   * @param inVertex Outgoing target vertex
   * @param fields Fields must be a odd pairs of key/value or a single object as Map containing
   *     entries as key/value pairs
   * @param iClassName Edge's class name
   * @param iClusterName The cluster name where to store the edge record
   * @return The new Edge created
   */
  public OrientEdge addEdge(
      String label,
      final OrientVertex inVertex,
      final String iClassName,
      final String iClusterName,
      final Object... fields) {
    if (inVertex == null) {
      throw new IllegalArgumentException("destination vertex is null");
    }
    final OrientBaseGraph graph = getGraph();
    if (graph != null) {
      return graph.addEdgeInternal(this, label, inVertex, iClassName, iClusterName, fields);
    }

    // IN MEMORY CHANGES ONLY: USE NOTX CLASS
    return OrientGraphNoTx.addEdgeInternal(
        null, this, label, inVertex, iClassName, iClusterName, fields);
  }

  /**
   * (Blueprints Extension) Returns the number of edges connected to the current Vertex.
   *
   * @param iDirection The direction between OUT, IN or BOTH
   * @param iLabels Optional labels as Strings to consider
   * @return A long with the total edges found
   */
  public long countEdges(final Direction iDirection, final String... iLabels) {
    checkIfAttached();

    long counter = 0;

    OrientBaseGraph.getEdgeClassNames(getGraph(), iLabels);
    OrientBaseGraph.encodeClassNames(iLabels);

    if (settings.isUseVertexFieldsForEdgeLabels() || iLabels == null || iLabels.length == 0) {
      // VERY FAST
      final ODocument doc = getRecord();
      for (String fieldName : doc.fieldNames()) {
        final OPair<Direction, String> connection = getConnection(iDirection, fieldName, iLabels);
        if (connection == null)
          // SKIP THIS FIELD
          continue;

        final Object fieldValue = doc.field(fieldName);
        if (fieldValue != null)
          if (fieldValue instanceof Collection<?>) counter += ((Collection<?>) fieldValue).size();
          else if (fieldValue instanceof Map<?, ?>) counter += ((Map<?, ?>) fieldValue).size();
          else if (fieldValue instanceof ORidBag) {
            counter += ((ORidBag) fieldValue).size();
          } else {
            counter++;
          }
      }
    } else {
      // SLOWER: BROWSE & FILTER
      for (Edge e : getEdges(iDirection, iLabels)) if (e != null) counter++;
    }
    return counter;
  }

  /**
   * Returns the edges connected to the current Vertex. If you are interested on just counting the
   * edges use @countEdges that it's more efficient for this use case.
   *
   * @param iDirection The direction between OUT, IN or BOTH
   * @param iLabels Optional labels as Strings to consider
   * @return
   */
  @Override
  public Iterable<Edge> getEdges(final Direction iDirection, final String... iLabels) {
    return getEdges(null, iDirection, iLabels);
  }

  /**
   * (Blueprints Extension) Returns all the edges from the current Vertex to another one.
   *
   * @param iDestination The target vertex
   * @param iDirection The direction between OUT, IN or BOTH
   * @param iLabels Optional labels as Strings to consider
   * @return
   */
  public Iterable<Edge> getEdges(
      final OrientVertex iDestination, final Direction iDirection, final String... iLabels) {

    setCurrentGraphInThreadLocal();

    final ODocument doc = getRecord();

    OrientBaseGraph.getEdgeClassNames(getGraph(), iLabels);
    OrientBaseGraph.encodeClassNames(iLabels);

    final OMultiCollectionIterator<Edge> iterable =
        new OMultiCollectionIterator<Edge>().setEmbedded(true);

    for (OTriple<String, Direction, String> connectionField :
        getConnectionFields(iDirection, iLabels)) {
      String fieldName = connectionField.getKey();
      OPair<Direction, String> connection = connectionField.getValue();

      final Object fieldValue = doc.rawField(fieldName);
      if (fieldValue != null) {
        final OIdentifiable destinationVId =
            iDestination != null ? (OIdentifiable) iDestination.getId() : null;

        if (fieldValue instanceof OIdentifiable) {
          addSingleEdge(doc, iterable, fieldName, connection, fieldValue, destinationVId, iLabels);

        } else if (fieldValue instanceof Collection<?>) {
          Collection<?> coll = (Collection<?>) fieldValue;

          if (coll.size() == 1) {
            // SINGLE ITEM: AVOID CALLING ITERATOR
            if (coll instanceof ORecordLazyMultiValue)
              addSingleEdge(
                  doc,
                  iterable,
                  fieldName,
                  connection,
                  ((ORecordLazyMultiValue) coll).rawIterator().next(),
                  destinationVId,
                  iLabels);
            else if (coll instanceof List<?>)
              addSingleEdge(
                  doc,
                  iterable,
                  fieldName,
                  connection,
                  ((List<?>) coll).get(0),
                  destinationVId,
                  iLabels);
            else
              addSingleEdge(
                  doc,
                  iterable,
                  fieldName,
                  connection,
                  coll.iterator().next(),
                  destinationVId,
                  iLabels);
          } else {
            // CREATE LAZY Iterable AGAINST COLLECTION FIELD
            if (coll instanceof ORecordLazyMultiValue) {
              iterable.add(
                  new OrientEdgeIterator(
                      this,
                      iDestination,
                      coll,
                      ((ORecordLazyMultiValue) coll).rawIterator(),
                      connection,
                      iLabels,
                      coll.size()));
            } else
              iterable.add(
                  new OrientEdgeIterator(
                      this, iDestination, coll, coll.iterator(), connection, iLabels, -1));
          }
        } else if (fieldValue instanceof ORidBag) {
          iterable.add(
              new OrientEdgeIterator(
                  this,
                  iDestination,
                  fieldValue,
                  ((ORidBag) fieldValue).rawIterator(),
                  connection,
                  iLabels,
                  ((ORidBag) fieldValue).size()));
        }
      }
    }

    return iterable;
  }

  /**
   * (Blueprints Extension) Returns the Vertex's label. By default OrientDB binds the Blueprints
   * Label concept to Vertex Class. To disable this feature execute this at database level <code>
   * alter database custom useClassForVertexLabel=false </code>
   */
  @Override
  public String getLabel() {
    setCurrentGraphInThreadLocal();

    if (settings.isUseClassForVertexLabel()) {
      final String clsName = getRecord().getClassName();
      if (!OrientVertexType.CLASS_NAME.equals(clsName))
        // RETURN THE CLASS NAME
        return clsName;
    }
    return getRecord().field(OrientElement.LABEL_FIELD_NAME);
  }

  /** (Blueprints Extension) Returns "V" as base class name all the vertex sub-classes extend. */
  @Override
  public String getBaseClassName() {
    return OrientVertexType.CLASS_NAME;
  }

  /** (Blueprints Extension) Returns "Vertex". */
  @Override
  public String getElementType() {
    return "Vertex";
  }

  /** (Blueprints Extension) Returns the Vertex type as OrientVertexType object. */
  @Override
  public OrientVertexType getType() {
    final OrientBaseGraph graph = getGraph();
    return new OrientVertexType(graph, getRecord().getSchemaClass());
  }

  /** Returns a string representation of the vertex. */
  public String toString() {
    setCurrentGraphInThreadLocal();

    final ODocument record = getRecord();
    if (record == null) return "<invalid record " + rawElement.getIdentity() + ">";

    final String clsName = record.getClassName();

    if (OrientVertexType.CLASS_NAME.equals(clsName)) return StringFactory.vertexString(this);

    return StringFactory.V
        + "("
        + clsName
        + ")"
        + StringFactory.L_BRACKET
        + getId()
        + StringFactory.R_BRACKET;
  }

  /**
   * Used to extract the class name from the vertex's field.
   *
   * @param iDirection Direction of connection
   * @param iFieldName Full field name
   * @return Class of the connection if any
   */
  public String getConnectionClass(final Direction iDirection, final String iFieldName) {
    if (iDirection == Direction.OUT) {
      if (iFieldName.length() > CONNECTION_OUT_PREFIX.length())
        return iFieldName.substring(CONNECTION_OUT_PREFIX.length());
    } else if (iDirection == Direction.IN) {
      if (iFieldName.length() > CONNECTION_IN_PREFIX.length())
        return iFieldName.substring(CONNECTION_IN_PREFIX.length());
    }
    return OrientEdgeType.CLASS_NAME;
  }

  /**
   * Determines if a field is a connections or not.
   *
   * @param iDirection Direction to check
   * @param iFieldName Field name
   * @param iClassNames Optional array of class names
   * @return The found direction if any
   */
  protected OPair<Direction, String> getConnection(
      final Direction iDirection, final String iFieldName, String... iClassNames) {
    if (iClassNames != null && iClassNames.length == 1 && iClassNames[0].equalsIgnoreCase("E"))
      // DEFAULT CLASS, TREAT IT AS NO CLASS/LABEL
      iClassNames = null;

    final OrientBaseGraph graph = getGraph();
    if (iDirection == Direction.OUT || iDirection == Direction.BOTH) {
      if (settings.isUseVertexFieldsForEdgeLabels()) {
        // FIELDS THAT STARTS WITH "out_"
        if (iFieldName.startsWith(CONNECTION_OUT_PREFIX)) {
          String connClass = getConnectionClass(Direction.OUT, iFieldName);
          if (iClassNames == null || iClassNames.length == 0)
            return new OPair<Direction, String>(Direction.OUT, connClass);

          // CHECK AGAINST ALL THE CLASS NAMES
          OrientEdgeType edgeType = graph.getEdgeType(connClass);
          if (edgeType != null) {
            for (String clsName : iClassNames) {
              if (edgeType.isSubClassOf(clsName))
                return new OPair<Direction, String>(Direction.OUT, connClass);
            }
          }
        }
      } else if (iFieldName.equals(OrientBaseGraph.CONNECTION_OUT))
        // CHECK FOR "out"
        return new OPair<Direction, String>(Direction.OUT, null);
    }

    if (iDirection == Direction.IN || iDirection == Direction.BOTH) {
      if (settings.isUseVertexFieldsForEdgeLabels()) {
        // FIELDS THAT STARTS WITH "in_"
        if (iFieldName.startsWith(CONNECTION_IN_PREFIX)) {
          String connClass = getConnectionClass(Direction.IN, iFieldName);
          if (iClassNames == null || iClassNames.length == 0)
            return new OPair<Direction, String>(Direction.IN, connClass);

          // CHECK AGAINST ALL THE CLASS NAMES
          OrientEdgeType edgeType = graph.getEdgeType(connClass);
          if (edgeType != null) {
            for (String clsName : iClassNames) {
              if (edgeType.isSubClassOf(clsName))
                return new OPair<Direction, String>(Direction.IN, connClass);
            }
          }
        }
      } else if (iFieldName.equals(OrientBaseGraph.CONNECTION_IN))
        // CHECK FOR "in"
        return new OPair<Direction, String>(Direction.IN, null);
    }

    // NOT FOUND
    return null;
  }

  /**
   * Returns all the possible fields names to look for.
   *
   * @param iDirection Direction to check
   * @param iClassNames Optional array of class names
   * @return The array of field names
   */
  private List<OTriple<String, Direction, String>> getConnectionFields(
      final Direction iDirection, String... iClassNames) {
    final ODocument doc = getRecord();
    List<OTriple<String, Direction, String>> result = new ArrayList<>();

    if (settings.isUseVertexFieldsForEdgeLabels()) {
      if (iClassNames == null
          || iClassNames.length == 0
          || (iClassNames.length == 1 && iClassNames[0].equalsIgnoreCase("E"))) {
        for (String fieldName : doc.fieldNames()) {
          final OPair<Direction, String> connection =
              getConnection(iDirection, fieldName, iClassNames);
          if (connection != null)
            result.add(
                new OTriple<String, Direction, String>(
                    fieldName, connection.getKey(), connection.getValue()));
        }
      } else {
        OSchema schema =
            ((ODatabaseDocumentInternal) getGraph().getRawGraph())
                .getMetadata()
                .getImmutableSchemaSnapshot();

        Set<String> allClassNames = new HashSet<String>();
        for (String className : iClassNames) {
          allClassNames.add(className);
          OClass clazz = schema.getClass(className);
          if (clazz != null) {
            Collection<OClass> subClasses = clazz.getAllSubclasses();
            for (OClass subClass : subClasses) {
              allClassNames.add(subClass.getName());
            }
          }
        }

        for (String className : allClassNames) {
          switch (iDirection) {
            case OUT:
              result.add(
                  new OTriple<String, Direction, String>(
                      CONNECTION_OUT_PREFIX + className, Direction.OUT, className));
              break;
            case IN:
              result.add(
                  new OTriple<String, Direction, String>(
                      CONNECTION_IN_PREFIX + className, Direction.IN, className));
              break;
            case BOTH:
              result.add(
                  new OTriple<String, Direction, String>(
                      CONNECTION_OUT_PREFIX + className, Direction.OUT, className));
              result.add(
                  new OTriple<String, Direction, String>(
                      CONNECTION_IN_PREFIX + className, Direction.IN, className));
              break;
          }
        }
      }
    } else {
      if (iDirection == Direction.OUT)
        result.add(
            new OTriple<String, Direction, String>(
                OrientBaseGraph.CONNECTION_OUT, Direction.OUT, null));
      else if (iDirection == Direction.IN)
        result.add(
            new OTriple<String, Direction, String>(
                OrientBaseGraph.CONNECTION_IN, Direction.IN, null));
      else {
        result.add(
            new OTriple<String, Direction, String>(
                OrientBaseGraph.CONNECTION_OUT, Direction.OUT, null));
        result.add(
            new OTriple<String, Direction, String>(
                OrientBaseGraph.CONNECTION_IN, Direction.IN, null));
      }
    }

    // EARLY FETCH ALL THE FIELDS THAT MATTERS
    String[] fieldNames = new String[result.size()];
    int i = 0;
    for (OTriple<String, Direction, String> connectionField : result)
      fieldNames[i++] = connectionField.getKey();
    doc.deserializeFields(fieldNames);

    return result;
  }

  protected void addSingleEdge(
      final ODocument doc,
      final OMultiCollectionIterator<Edge> iterable,
      String fieldName,
      final OPair<Direction, String> connection,
      final Object fieldValue,
      final OIdentifiable iTargetVertex,
      final String[] iLabels) {
    final OrientBaseGraph graph = getGraph();
    final OrientEdge toAdd =
        getEdge(graph, doc, fieldName, connection, fieldValue, iTargetVertex, iLabels);

    if (toAdd != null && (settings.isUseVertexFieldsForEdgeLabels() || toAdd.isLabeled(iLabels)))
      // ADD THE EDGE
      iterable.add(toAdd);
  }

  boolean canCreateDynamicEdge(
      final ODocument iFromVertex,
      final ODocument iToVertex,
      final String iOutFieldName,
      final String iInFieldName,
      final Object[] fields,
      final String label) {

    checkIfAttached();

    final OrientBaseGraph graph = getGraph();
    if (!settings.isUseVertexFieldsForEdgeLabels() && label != null) return false;

    if (settings.isUseLightweightEdges()
        && (fields == null
            || fields.length == 0
            || fields[0] == null
            || (fields[0] instanceof Map && ((Map) fields[0]).isEmpty()))) {
      Object field = iFromVertex.field(iOutFieldName);
      if (field != null)
        if (field instanceof Collection<?>)
          if (((Collection<Object>) field).contains(iToVertex)) {
            // ALREADY EXISTS, FORCE THE EDGE-DOCUMENT TO AVOID
            // MULTIPLE DYN-EDGES AGAINST THE SAME VERTICES
            graph.getEdgeInstance(iFromVertex, iToVertex, label).convertToDocument();
            return false;
          }

      field = iToVertex.field(iInFieldName);
      if (field != null)
        if (field instanceof Collection<?>)
          if (((Collection<Object>) field).contains(iFromVertex)) {
            // ALREADY EXISTS, FORCE THE EDGE-DOCUMENT TO AVOID
            // MULTIPLE DYN-EDGES AGAINST THE SAME VERTICES
            graph.getEdgeInstance(iFromVertex, iToVertex, label).convertToDocument();
            return false;
          }

      if (settings.isUseClassForEdgeLabel()) {
        // CHECK IF THE EDGE CLASS HAS SPECIAL CONSTRAINTS
        final OClass cls = graph.getEdgeType(label);
        if (cls != null)
          for (OProperty p : cls.properties()) {
            if (p.isMandatory()
                || p.isNotNull()
                || !p.getOwnerClass().getInvolvedIndexes(p.getName()).isEmpty()) return false;
          }
      }

      // CAN USE DYNAMIC EDGES
      return true;
    }
    return false;
  }

  private void addSingleVertex(
      final ODocument doc,
      final OMultiCollectionIterator<Vertex> iterable,
      String fieldName,
      final OPair<Direction, String> connection,
      final Object fieldValue,
      final String[] iLabels) {
    final OrientBaseGraph graph = getGraph();

    final OrientVertex toAdd;

    final ODocument fieldRecord = ((OIdentifiable) fieldValue).getRecord();
    OClass klass = ODocumentInternal.getImmutableSchemaClass(fieldRecord);
    if (klass == null && ODatabaseRecordThreadLocal.instance().getIfDefined() != null) {
      ODatabaseRecordThreadLocal.instance().getIfDefined().getMetadata().reload();
      klass = fieldRecord.getSchemaClass();
    }
    if (klass.isVertexType()) {
      // DIRECT VERTEX
      toAdd = graph.getVertex(fieldRecord);
    } else if (klass.isEdgeType()) {
      // EDGE
      if (settings.isUseVertexFieldsForEdgeLabels()
          || OrientEdge.isLabeled(OrientEdge.getRecordLabel(fieldRecord), iLabels)) {
        OIdentifiable vertexDoc =
            OrientEdge.getConnection(fieldRecord, connection.getKey().opposite());
        if (vertexDoc == null) {

          fieldRecord.reload();
          vertexDoc = OrientEdge.getConnection(fieldRecord, connection.getKey().opposite());

          if (vertexDoc == null) {
            OLogManager.instance()
                .warn(
                    this,
                    "Cannot load edge "
                        + fieldRecord
                        + " to get the "
                        + connection.getKey().opposite()
                        + " vertex");
            return;
          }
        }

        toAdd = graph.getVertex(vertexDoc);
      } else toAdd = null;
    } else
      throw new IllegalStateException(
          "Invalid content found in " + fieldName + " field: " + fieldRecord);

    if (toAdd != null)
      // ADD THE VERTEX
      iterable.add(toAdd);
  }
}
