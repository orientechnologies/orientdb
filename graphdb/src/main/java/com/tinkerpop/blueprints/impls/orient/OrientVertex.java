package com.tinkerpop.blueprints.impls.orient;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.orientechnologies.common.collection.OMultiCollectionIterator;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.command.traverse.OTraverse;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordLazyMultiValue;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Index;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.ExceptionFactory;
import com.tinkerpop.blueprints.util.StringFactory;

/**
 * @author Luca Garulli (http://www.orientechnologies.com)
 */
@SuppressWarnings("unchecked")
public class OrientVertex extends OrientElement implements Vertex {
  public static final String CLASS_NAME            = "V";
  public static final String CONNECTION_OUT_PREFIX = OrientBaseGraph.CONNECTION_OUT + "_";
  public static final String CONNECTION_IN_PREFIX  = OrientBaseGraph.CONNECTION_IN + "_";

  private static final long  serialVersionUID      = 1L;

  protected OrientVertex(final OrientBaseGraph graph, String className, final Object... fields) {
    super(graph, null);
    if (className != null)
      className = checkForClassInSchema(OrientBaseGraph.encodeClassName(className));

    rawElement = new ODocument(className == null ? CLASS_NAME : className);
    setProperties(fields);
  }

  protected OrientVertex(final OrientBaseGraph graph, final OIdentifiable record) {
    super(graph, record);
  }

  @Override
  public Set<String> getPropertyKeys() {
    graph.setCurrentGraphInThreadLocal();

    final ODocument doc = getRecord();

    final Set<String> result = new HashSet<String>();
    for (String field : doc.fieldNames())
      if (graph.isUseVertexFieldsForEdgeLabels()) {
        if (!field.startsWith(CONNECTION_OUT_PREFIX) && !field.startsWith(CONNECTION_IN_PREFIX))
          result.add(field);
      } else if (!field.equals(OrientBaseGraph.CONNECTION_OUT) && !field.equals(OrientBaseGraph.CONNECTION_IN))
        result.add(field);

    return result;
  }

  /**
   * Returns a lazy iterable instance against vertices.
   */
  @Override
  public Iterable<Vertex> getVertices(final Direction iDirection, final String... iLabels) {
    graph.setCurrentGraphInThreadLocal();

    OrientBaseGraph.encodeClassNames(iLabels);

    final ODocument doc = getRecord();

    final OMultiCollectionIterator<Vertex> iterable = new OMultiCollectionIterator<Vertex>();
    for (String fieldName : doc.fieldNames()) {
      final OPair<Direction, String> connection = getConnection(iDirection, fieldName, iLabels);
      if (connection == null)
        // SKIP THIS FIELD
        continue;

      final Object fieldValue = doc.field(fieldName);
      if (fieldValue != null)
        if (fieldValue instanceof OIdentifiable) {
          addSingleVertex(doc, iterable, fieldName, connection, fieldValue, iLabels);

        } else if (fieldValue instanceof Collection<?>) {
          Collection<?> coll = (Collection<?>) fieldValue;

          if (coll.size() == 1) {
            // SINGLE ITEM: AVOID CALLING ITERATOR
            if (coll instanceof ORecordLazyMultiValue)
              addSingleVertex(doc, iterable, fieldName, connection, ((ORecordLazyMultiValue) coll).rawIterator().next(), iLabels);
            else if (coll instanceof List<?>)
              addSingleVertex(doc, iterable, fieldName, connection, ((List<?>) coll).get(0), iLabels);
            else
              addSingleVertex(doc, iterable, fieldName, connection, coll.iterator().next(), iLabels);
          } else {
            // CREATE LAZY Iterable AGAINST COLLECTION FIELD
            if (coll instanceof ORecordLazyMultiValue)
              iterable.add(new OrientVertexIterator(this, ((ORecordLazyMultiValue) coll).rawIterator(), connection, iLabels,
                  ((ORecordLazyMultiValue) coll).size()));
            else
              iterable.add(new OrientVertexIterator(this, coll.iterator(), connection, iLabels, -1));
          }
        } else if (fieldValue instanceof ORidBag) {
          iterable.add(new OrientVertexIterator(this, ((ORidBag) fieldValue).rawIterator(), connection, iLabels, -1));
        }
    }

    return iterable;
  }

  @Override
  public OrientVertexQuery query() {
    graph.setCurrentGraphInThreadLocal();

    return new OrientVertexQuery(this);
  }

  /**
   * Returns a OTraverse object to start traversing from the current vertex.
   */
  public OTraverse traverse() {
    graph.setCurrentGraphInThreadLocal();

    return new OTraverse().target(getRecord());
  }

  @Override
  public void remove() {
    checkClass();

    graph.autoStartTransaction();

    final ODocument doc = getRecord();
    if (doc == null)
      throw ExceptionFactory.vertexWithIdDoesNotExist(this.getId());

    final Iterator<OrientIndex<? extends OrientElement>> it = graph.getManualIndices().iterator();

    if (it.hasNext()) {
      final Set<Edge> allEdges = new HashSet<Edge>();
      for (Edge e : getEdges(Direction.BOTH))
        allEdges.add(e);

      while (it.hasNext()) {
        final Index<? extends Element> index = it.next();

        if (Vertex.class.isAssignableFrom(index.getIndexClass())) {
          OrientIndex<OrientVertex> idx = (OrientIndex<OrientVertex>) index;
          idx.removeElement(this);
        }

        if (Edge.class.isAssignableFrom(index.getIndexClass())) {
          OrientIndex<OrientEdge> idx = (OrientIndex<OrientEdge>) index;
          for (Edge e : allEdges)
            idx.removeElement((OrientEdge) e);
        }
      }
    }

    for (String fieldName : doc.fieldNames()) {
      final OPair<Direction, String> connection = getConnection(Direction.BOTH, fieldName);
      if (connection == null)
        // SKIP THIS FIELD
        continue;

      removeEdges(doc, fieldName, null, true, graph.isUseVertexFieldsForEdgeLabels());
    }

    super.remove();
  }

  @Override
  public Edge addEdge(final String label, final Vertex inVertex) {
    return addEdge(label, (OrientVertex) inVertex, null, null, (Object[]) null);
  }

  public OrientEdge addEdge(final String label, final OrientVertex inVertex, final String iClassName) {
    return addEdge(label, inVertex, iClassName, null, (Object[]) null);
  }

  public OrientEdge addEdge(final String label, final OrientVertex inVertex, final Object[] fields) {
    return addEdge(label, inVertex, null, null, fields);
  }

  public OrientEdge addEdge(String label, final OrientVertex inVertex, final String iClassName, final String iClusterName,
      final Object... fields) {
    if (inVertex == null)
      throw new IllegalArgumentException("destination vertex is null");

    graph.autoStartTransaction();

    // TEMPORARY STATIC LOCK TO AVOID MT PROBLEMS AGAINST OMVRBTreeRID
    final ODocument outDocument = getRecord();
    final ODocument inDocument = ((OrientVertex) inVertex).getRecord();

    final OrientEdge edge;
    OIdentifiable to;
    OIdentifiable from;

    label = OrientBaseGraph.encodeClassName(label);
    if (label == null && iClassName != null)
      // RETRO-COMPATIBILITY WITH THE SYNTAX CLASS:<CLASS-NAME>
      label = OrientBaseGraph.encodeClassName(iClassName);

    final boolean useVertexFieldsForEdgeLabels = graph.isUseVertexFieldsForEdgeLabels();
    final String outFieldName = getConnectionFieldName(Direction.OUT, label, useVertexFieldsForEdgeLabels);
    final String inFieldName = getConnectionFieldName(Direction.IN, label, useVertexFieldsForEdgeLabels);

    // since the label for the edge can potentially get re-assigned
    // before being pushed into the OrientEdge, the
    // null check has to go here.
    if (label == null)
      throw ExceptionFactory.edgeLabelCanNotBeNull();

    if (canCreateDynamicEdge(outDocument, inDocument, outFieldName, inFieldName, fields, label)) {
      // CREATE A LIGHTWEIGHT DYNAMIC EDGE
      from = rawElement;
      to = inDocument;
      edge = new OrientEdge(graph, from, to, label);
    } else {
      // CREATE THE EDGE DOCUMENT TO STORE FIELDS TOO
      edge = new OrientEdge(graph, label, fields);

      if (graph.isKeepInMemoryReferences())
        edge.getRecord().fields(OrientBaseGraph.CONNECTION_OUT, rawElement.getIdentity(), OrientBaseGraph.CONNECTION_IN,
            inDocument.getIdentity());
      else
        edge.getRecord().fields(OrientBaseGraph.CONNECTION_OUT, rawElement, OrientBaseGraph.CONNECTION_IN, inDocument);

      from = (OIdentifiable) edge.getRecord();
      to = (OIdentifiable) edge.getRecord();
    }

    if (graph.isKeepInMemoryReferences()) {
      // USES REFERENCES INSTEAD OF DOCUMENTS
      from = from.getIdentity();
      to = to.getIdentity();
    }

    // OUT-VERTEX ---> IN-VERTEX/EDGE
    createLink(outDocument, to, outFieldName);

    // IN-VERTEX ---> OUT-VERTEX/EDGE
    createLink(inDocument, from, inFieldName);

    edge.save(iClusterName);
    inDocument.save();
    outDocument.save();

    return edge;

  }

  private boolean canCreateDynamicEdge(final ODocument iFromVertex, final ODocument iToVertex, final String iOutFieldName,
      final String iInFieldName, final Object[] fields, final String label) {

    if (!graph.isUseVertexFieldsForEdgeLabels() && label != null)
      return false;

    if (graph.isUseLightweightEdges() && (fields == null || fields.length == 0 || fields[0] == null)) {
      Object field = iFromVertex.field(iOutFieldName);
      if (field != null)
        if (field instanceof OIdentifiable) {
          if (field.equals(iToVertex)) {
            // ALREADY EXISTS, FORCE THE EDGE-DOCUMENT TO AVOID
            // MULTIPLE DYN-EDGES AGAINST THE SAME VERTICES
            new OrientEdge(graph, iFromVertex, iToVertex, label).convertToDocument();
            return false;
          }
        }

      field = iToVertex.field(iInFieldName);
      if (field != null)
        if (field instanceof OIdentifiable) {
          if (field.equals(iFromVertex)) {
            // ALREADY EXISTS, FORCE THE EDGE-DOCUMENT TO AVOID
            // MULTIPLE DYN-EDGES AGAINST THE SAME VERTICES
            new OrientEdge(graph, iFromVertex, iToVertex, label).convertToDocument();
            return false;
          }
        }

      if (graph.isUseClassForEdgeLabel()) {
        // CHECK IF THE EDGE CLASS HAS SPECIAL CONSTRAINTS
        final OClass cls = graph.getEdgeType(label);
        if (cls != null)
          for (OProperty p : cls.properties()) {
            if (p.isMandatory() || p.isNotNull())
              return false;
          }
      }

      // CAN USE DYNAMIC EDGES
      return true;
    }
    return false;
  }

  public long countEdges(final Direction iDirection, final String... iLabels) {
    long counter = 0;

    OrientBaseGraph.encodeClassNames(iLabels);

    if (graph.isUseVertexFieldsForEdgeLabels() || iLabels == null || iLabels.length == 0) {
      // VERY FAST
      final ODocument doc = getRecord();
      for (String fieldName : doc.fieldNames()) {
        final OPair<Direction, String> connection = getConnection(iDirection, fieldName, iLabels);
        if (connection == null)
          // SKIP THIS FIELD
          continue;

        final Object fieldValue = doc.field(fieldName);
        if (fieldValue != null)
          if (fieldValue instanceof Collection<?>)
            counter += ((Collection<?>) fieldValue).size();
          else if (fieldValue instanceof Map<?, ?>)
            counter += ((Map<?, ?>) fieldValue).size();
          else if (fieldValue instanceof ORidBag) {
            counter += ((ORidBag) fieldValue).size();
          } else {
            counter++;
          }
      }
    } else {
      // SLOWER: BROWSE & FILTER
      for (Edge e : getEdges(iDirection, iLabels))
        if (e != null)
          counter++;
    }
    return counter;
  }

  public Iterable<Edge> getEdges(final Direction iDirection, final String... iLabels) {
    return getEdges(null, iDirection, iLabels);
  }

  public Iterable<Edge> getEdges(final OrientVertex iDestination, final Direction iDirection, final String... iLabels) {

    graph.setCurrentGraphInThreadLocal();

    final ODocument doc = getRecord();

    OrientBaseGraph.encodeClassNames(iLabels);

    final OMultiCollectionIterator<Edge> iterable = new OMultiCollectionIterator<Edge>().setEmbedded(true);
    for (String fieldName : doc.fieldNames()) {
      final OPair<Direction, String> connection = getConnection(iDirection, fieldName, iLabels);
      if (connection == null)
        // SKIP THIS FIELD
        continue;

      final Object fieldValue = doc.field(fieldName);
      if (fieldValue != null) {
        final OIdentifiable destinationVId = iDestination != null ? (OIdentifiable) iDestination.getId() : null;

        if (fieldValue instanceof OIdentifiable) {
          addSingleEdge(doc, iterable, fieldName, connection, fieldValue, destinationVId, iLabels);

        } else if (fieldValue instanceof Collection<?>) {
          Collection<?> coll = (Collection<?>) fieldValue;

          if (coll.size() == 1) {
            // SINGLE ITEM: AVOID CALLING ITERATOR
            if (coll instanceof ORecordLazyMultiValue)
              addSingleEdge(doc, iterable, fieldName, connection, ((ORecordLazyMultiValue) coll).rawIterator().next(),
                  destinationVId, iLabels);
            else if (coll instanceof List<?>)
              addSingleEdge(doc, iterable, fieldName, connection, ((List<?>) coll).get(0), destinationVId, iLabels);
            else
              addSingleEdge(doc, iterable, fieldName, connection, coll.iterator().next(), destinationVId, iLabels);
          } else {
            // CREATE LAZY Iterable AGAINST COLLECTION FIELD
            if (coll instanceof ORecordLazyMultiValue) {
              iterable.add(new OrientEdgeIterator(this, iDestination, ((ORecordLazyMultiValue) coll).rawIterator(), connection,
                  iLabels, ((ORecordLazyMultiValue) coll).size()));
            } else
              iterable.add(new OrientEdgeIterator(this, iDestination, coll.iterator(), connection, iLabels, -1));
          }
        } else if (fieldValue instanceof ORidBag) {
          ORidBag bag = (ORidBag) fieldValue;

          iterable.add(new OrientEdgeIterator(this, iDestination, bag.rawIterator(), connection, iLabels, -1));
        }
      }
    }

    return iterable;
  }

  public String getLabel() {
    graph.setCurrentGraphInThreadLocal();

    if (graph.isUseClassForVertexLabel()) {
      final String clsName = getRecord().getClassName();
      if (!CLASS_NAME.equals(clsName))
        // RETURN THE CLASS NAME
        return clsName;
    }
    return getRecord().field(OrientElement.LABEL_FIELD_NAME);
  }

  @Override
  public String getBaseClassName() {
    return CLASS_NAME;
  }

  @Override
  public String getElementType() {
    return "Vertex";
  }

  public String toString() {
    graph.setCurrentGraphInThreadLocal();

    final String clsName = getRecord().getClassName();

    if (clsName.equals(CLASS_NAME))
      return StringFactory.vertexString(this);

    return StringFactory.V + "(" + clsName + ")" + StringFactory.L_BRACKET + getId() + StringFactory.R_BRACKET;
  }

  /**
   * Determines if a field is a connections or not.
   * 
   * @param iDirection
   *          Direction to check
   * @param iFieldName
   *          Field name
   * @param iClassNames
   *          Optional array of class names
   * @return The found direction if any
   */
  protected OPair<Direction, String> getConnection(final Direction iDirection, final String iFieldName, final String... iClassNames) {
    if (iDirection == Direction.OUT || iDirection == Direction.BOTH) {
      if (graph.isUseVertexFieldsForEdgeLabels()) {
        // FIELDS THAT STARTS WITH "out_"
        if (iFieldName.startsWith(CONNECTION_OUT_PREFIX)) {
          if (iClassNames == null || iClassNames.length == 0)
            return new OPair<Direction, String>(Direction.OUT, getConnectionClass(Direction.OUT, iFieldName));

          // CHECK AGAINST ALL THE CLASS NAMES
          for (String clsName : iClassNames) {
            clsName = OrientBaseGraph.encodeClassName(clsName);

            if (iFieldName.equals(CONNECTION_OUT_PREFIX + clsName))
              return new OPair<Direction, String>(Direction.OUT, clsName);
          }
        }
      } else if (iFieldName.equals(OrientBaseGraph.CONNECTION_OUT))
        // CHECK FOR "out"
        return new OPair<Direction, String>(Direction.OUT, null);
    }

    if (iDirection == Direction.IN || iDirection == Direction.BOTH) {
      if (graph.isUseVertexFieldsForEdgeLabels()) {
        // FIELDS THAT STARTS WITH "in_"
        if (iFieldName.startsWith(CONNECTION_IN_PREFIX)) {
          if (iClassNames == null || iClassNames.length == 0)
            return new OPair<Direction, String>(Direction.IN, getConnectionClass(Direction.IN, iFieldName));

          // CHECK AGAINST ALL THE CLASS NAMES
          for (String clsName : iClassNames) {
            if (iFieldName.equals(CONNECTION_IN_PREFIX + clsName))
              return new OPair<Direction, String>(Direction.IN, clsName);
          }
        }
      } else if (iFieldName.equals(OrientBaseGraph.CONNECTION_IN))
        // CHECK FOR "in"
        return new OPair<Direction, String>(Direction.IN, null);
    }

    // NOT FOUND
    return null;
  }

  public static String getConnectionFieldName(final Direction iDirection, final String iClassName,
      final boolean useVertexFieldsForEdgeLabels) {
    if (iDirection == null || iDirection == Direction.BOTH)
      throw new IllegalArgumentException("Direction not valid");

    if (useVertexFieldsForEdgeLabels) {
      // PREFIX "out_" or "in_" TO THE FIELD NAME
      final String prefix = iDirection == Direction.OUT ? CONNECTION_OUT_PREFIX : CONNECTION_IN_PREFIX;
      if (iClassName == null || iClassName.isEmpty() || iClassName.equals(OrientEdge.CLASS_NAME))
        return prefix;

      return prefix + iClassName;
    } else
      // "out" or "in"
      return iDirection == Direction.OUT ? OrientBaseGraph.CONNECTION_OUT : OrientBaseGraph.CONNECTION_IN;
  }

  public static Object createLink(final ODocument iFromVertex, final OIdentifiable iTo, final String iFieldName) {
    final Object out;
    Object found = iFromVertex.field(iFieldName);
    if (found == null)
      // CREATE ONLY ONE LINK
      out = iTo;
    else if (found instanceof OIdentifiable) {
      // DOUBLE: SCALE UP THE LINK INTO A COLLECTION
      final ORidBag bag = new ORidBag();
      bag.add((OIdentifiable) found);
      bag.add(iTo);
      out = bag;
    } else if (found instanceof ORidBag) {
      // ADD THE LINK TO THE COLLECTION
      out = null;
      ((ORidBag) found).add(iTo);
    } else if (found instanceof Collection<?>) {
      // CONVERT IT TO BAG
      final ORidBag bag = new ORidBag();
      bag.addAll((Collection<OIdentifiable>) found);
      bag.add(iTo);
      out = bag;

    } else
      throw new IllegalStateException("Relationship content is invalid on field " + iFieldName + ". Found: " + found);

    if (out != null)
      // OVERWRITE IT
      iFromVertex.field(iFieldName, out);

    return out;
  }

  public static Direction getConnectionDirection(final String iConnectionField, final boolean useVertexFieldsForEdgeLabels) {
    if (iConnectionField == null)
      throw new IllegalArgumentException("Cannot return direction of NULL connection ");

    if (useVertexFieldsForEdgeLabels) {
      if (iConnectionField.startsWith(CONNECTION_OUT_PREFIX))
        return Direction.OUT;
      else if (iConnectionField.startsWith(CONNECTION_IN_PREFIX))
        return Direction.IN;
    } else {
      if (iConnectionField.equals(OrientBaseGraph.CONNECTION_OUT))
        return Direction.OUT;
      else if (iConnectionField.startsWith(OrientBaseGraph.CONNECTION_IN))
        return Direction.IN;
    }

    throw new IllegalArgumentException("Cannot return direction of connection " + iConnectionField);
  }

  /**
   * Used to extract the class name from the vertex's field.
   * 
   * @param iDirection
   *          Direction of connection
   * @param iFieldName
   *          Full field name
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
    return OrientEdge.CLASS_NAME;
  }

  public static String getInverseConnectionFieldName(final String iFieldName, final boolean useVertexFieldsForEdgeLabels) {
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
        throw new IllegalArgumentException("Cannot find reverse connection name for field " + iFieldName);
    }

    if (iFieldName.equals(OrientBaseGraph.CONNECTION_OUT))
      return OrientBaseGraph.CONNECTION_IN;
    else if (iFieldName.equals(OrientBaseGraph.CONNECTION_IN))
      return OrientBaseGraph.CONNECTION_OUT;

    throw new IllegalArgumentException("Cannot find reverse connection name for field " + iFieldName);
  }

  public static void removeEdges(final ODocument iVertex, final String iFieldName, final OIdentifiable iVertexToRemove,
      final boolean iAlsoInverse, final boolean useVertexFieldsForEdgeLabels) {
    if (iVertex == null)
      return;

    final Object fieldValue = iVertexToRemove != null ? iVertex.field(iFieldName) : iVertex.removeField(iFieldName);
    if (fieldValue == null)
      return;

    if (fieldValue instanceof OIdentifiable) {
      // SINGLE RECORD

      if (iVertexToRemove != null) {
        if (!fieldValue.equals(iVertexToRemove)) {
          // OLogManager.instance().warn(null,
          // "[OrientVertex.removeEdges] connections %s not found in field %s",
          // iVertexToRemove,
          // iFieldName);
          return;
        }
        iVertex.removeField(iFieldName);
      }

      if (iAlsoInverse)
        removeInverseEdge(iVertex, iFieldName, iVertexToRemove, fieldValue, useVertexFieldsForEdgeLabels);

      deleteEdgeIfAny((OIdentifiable) fieldValue);

    } else if (fieldValue instanceof ORidBag) {
      // COLLECTION OF RECORDS: REMOVE THE ENTRY
      final ORidBag bag = (ORidBag) fieldValue;

      if (iVertexToRemove != null) {
        // SEARCH SEQUENTIALLY (SLOWER)
        boolean found = false;
        for (Iterator<OIdentifiable> it = bag.rawIterator(); it.hasNext();) {
          final ODocument curr = it.next().getRecord();

          if (iVertexToRemove.equals(curr)) {
            // FOUND AS VERTEX
            it.remove();
            if (iAlsoInverse)
              removeInverseEdge(iVertex, iFieldName, iVertexToRemove, curr, useVertexFieldsForEdgeLabels);
            found = true;
            break;

          } else if (curr.getSchemaClass().isSubClassOf(OrientEdge.CLASS_NAME)) {
            final Direction direction = getConnectionDirection(iFieldName, useVertexFieldsForEdgeLabels);

            // EDGE, REMOVE THE EDGE
            if (iVertexToRemove.equals(OrientEdge.getConnection(curr, direction.opposite()))) {
              it.remove();
              if (iAlsoInverse)
                removeInverseEdge(iVertex, iFieldName, iVertexToRemove, curr, useVertexFieldsForEdgeLabels);
              found = true;
              break;
            }
          }
        }

        if (!found)
          OLogManager.instance()
              .warn(null, "[OrientVertex.removeEdges] edge %s not found in field %s", iVertexToRemove, iFieldName);

        deleteEdgeIfAny(iVertexToRemove);

      } else {

        // DELETE ALL THE EDGES
        for (Iterator<OIdentifiable> it = bag.rawIterator(); it.hasNext();) {
          final OIdentifiable edge = it.next();

          if (iAlsoInverse)
            removeInverseEdge(iVertex, iFieldName, null, edge, useVertexFieldsForEdgeLabels);

          deleteEdgeIfAny(edge);
        }
      }

      if (bag.isEmpty())
        // FORCE REMOVAL OF ENTIRE FIELD
        iVertex.removeField(iFieldName);
    }

    iVertex.save();
  }

  private static void deleteEdgeIfAny(final OIdentifiable iRecord) {
    if (iRecord != null) {
      final ODocument doc = iRecord.getRecord();
      if (doc != null && doc.getSchemaClass() != null && doc.getSchemaClass().isSubClassOf(OrientEdge.CLASS_NAME))
        // DELETE THE EDGE RECORD TOO
        doc.delete();
    }
  }

  private static void removeInverseEdge(final ODocument iVertex, final String iFieldName, final OIdentifiable iVertexToRemove,
      final Object iFieldValue, final boolean useVertexFieldsForEdgeLabels) {
    final ODocument r = ((OIdentifiable) iFieldValue).getRecord();

    if (r == null)
      return;

    final String inverseFieldName = getInverseConnectionFieldName(iFieldName, useVertexFieldsForEdgeLabels);
    if (r.getSchemaClass().isSubClassOf(CLASS_NAME)) {
      // DIRECT VERTEX
      removeEdges(r, inverseFieldName, iVertex, false, useVertexFieldsForEdgeLabels);

    } else if (r.getSchemaClass().isSubClassOf(OrientEdge.CLASS_NAME)) {
      // EDGE, REMOVE THE EDGE
      final OIdentifiable otherVertex = OrientEdge.getConnection(r,
          getConnectionDirection(inverseFieldName, useVertexFieldsForEdgeLabels));

      if (otherVertex != null) {
        if (iVertexToRemove == null || otherVertex.equals(iVertexToRemove))
          // BIDIRECTIONAL EDGE
          removeEdges((ODocument) otherVertex.getRecord(), inverseFieldName, (OIdentifiable) iFieldValue, false,
              useVertexFieldsForEdgeLabels);

      } else
        throw new IllegalStateException("Invalid content found in " + iFieldName + " field");
    }
  }

  private void addSingleVertex(final ODocument doc, final OMultiCollectionIterator<Vertex> iterable, String fieldName,
      final OPair<Direction, String> connection, final Object fieldValue, final String[] iLabels) {
    final OrientVertex toAdd;

    final ODocument fieldRecord = ((OIdentifiable) fieldValue).getRecord();
    if (fieldRecord.getSchemaClass().isSubClassOf(CLASS_NAME)) {
      // DIRECT VERTEX
      toAdd = new OrientVertex(graph, fieldRecord);
    } else if (fieldRecord.getSchemaClass().isSubClassOf(OrientEdge.CLASS_NAME)) {
      // EDGE
      if (graph.isUseVertexFieldsForEdgeLabels() || OrientEdge.isLabeled(OrientEdge.getRecordLabel(fieldRecord), iLabels)) {
        final OIdentifiable vertexDoc = OrientEdge.getConnection(fieldRecord, connection.getKey().opposite());
        if (vertexDoc == null) {
          fieldRecord.reload();
          if (vertexDoc == null) {
            OLogManager.instance().warn(this,
                "Cannot load edge " + fieldRecord + " to get the " + connection.getKey().opposite() + " vertex");
            return;
          }
        }

        toAdd = new OrientVertex(graph, vertexDoc);
      } else
        toAdd = null;
    } else
      throw new IllegalStateException("Invalid content found in " + fieldName + " field: " + fieldRecord);

    if (toAdd != null)
      // ADD THE VERTEX
      iterable.add(toAdd);
  }

  private void addSingleEdge(final ODocument doc, final OMultiCollectionIterator<Edge> iterable, String fieldName,
      final OPair<Direction, String> connection, final Object fieldValue, final OIdentifiable iTargetVertex, final String[] iLabels) {
    final OrientEdge toAdd;

    final ODocument fieldRecord = ((OIdentifiable) fieldValue).getRecord();
    if (fieldRecord.getSchemaClass().isSubClassOf(CLASS_NAME)) {
      if (iTargetVertex != null && !iTargetVertex.equals(fieldValue))
        return;

      // DIRECT VERTEX, CREATE A DUMMY EDGE BETWEEN VERTICES
      if (connection.getKey() == Direction.OUT)
        toAdd = new OrientEdge(graph, doc, fieldRecord, connection.getValue());
      else
        toAdd = new OrientEdge(graph, fieldRecord, doc, connection.getValue());

    } else if (fieldRecord.getSchemaClass().isSubClassOf(OrientEdge.CLASS_NAME)) {
      // EDGE
      if (iTargetVertex != null) {
        Object targetVertex = OrientEdge.getConnection(fieldRecord, connection.getKey().opposite());

        if (!iTargetVertex.equals(targetVertex))
          return;
      }

      toAdd = new OrientEdge(graph, fieldRecord);
    } else
      throw new IllegalStateException("Invalid content found in " + fieldName + " field: " + fieldRecord);

    if (graph.isUseVertexFieldsForEdgeLabels() || toAdd.isLabeled(iLabels))
      // ADD THE EDGE
      iterable.add(toAdd);
  }
}