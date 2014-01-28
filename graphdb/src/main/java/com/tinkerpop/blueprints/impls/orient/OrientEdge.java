package com.tinkerpop.blueprints.impls.orient;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Index;
import com.tinkerpop.blueprints.util.ExceptionFactory;
import com.tinkerpop.blueprints.util.StringFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Luca Garulli (http://www.orientechnologies.com)
 */
@SuppressWarnings("unchecked")
public class OrientEdge extends OrientElement implements Edge {
  private static final long serialVersionUID = 1L;

  protected OIdentifiable   vOut;
  protected OIdentifiable   vIn;
  protected String          label;

  public OrientEdge(final OrientBaseGraph rawGraph, final OIdentifiable rawEdge) {
    super(rawGraph, rawEdge);
  }

  public OrientEdge(final OrientBaseGraph rawGraph, final String iLabel, final Object... fields) {
    super(rawGraph, null);
    rawElement = createDocument(iLabel);
    setProperties(fields);
  }

  public OrientEdge(final OrientBaseGraph rawGraph, final OIdentifiable out, final OIdentifiable in) {
    this(rawGraph, out, in, null);
  }

  public OrientEdge(final OrientBaseGraph rawGraph, final OIdentifiable out, final OIdentifiable in, final String iLabel) {
    super(rawGraph, null);
    vOut = out;
    vIn = in;
    label = iLabel;
  }

  @Override
  public OrientVertex getVertex(final Direction direction) {
    graph.setCurrentGraphInThreadLocal();

    if (direction.equals(Direction.OUT))
      return new OrientVertex(graph, getOutVertex());
    else if (direction.equals(Direction.IN))
      return new OrientVertex(graph, getInVertex());
    else
      throw ExceptionFactory.bothIsNotSupported();
  }

  public OIdentifiable getOutVertex() {
    if (vOut != null)
      // LIGHTWEIGHT EDGE
      return vOut;

    graph.setCurrentGraphInThreadLocal();

    final ODocument doc = getRecord();
    if (doc == null)
      return null;

    if (graph.isKeepInMemoryReferences())
      // AVOID LAZY RESOLVING+SETTING OF RECORD
      return doc.rawField(OrientBaseGraph.CONNECTION_OUT);
    else
      return doc.field(OrientBaseGraph.CONNECTION_OUT);
  }

  public OIdentifiable getInVertex() {
    if (vIn != null)
      // LIGHTWEIGHT EDGE
      return vIn;

    graph.setCurrentGraphInThreadLocal();

    final ODocument doc = getRecord();
    if (doc == null)
      return null;

    if (graph.isKeepInMemoryReferences())
      // AVOID LAZY RESOLVING+SETTING OF RECORD
      return doc.rawField(OrientBaseGraph.CONNECTION_IN);
    else
      return doc.field(OrientBaseGraph.CONNECTION_IN);
  }

  @Override
  public String getLabel() {
    if (label != null)
      // LIGHTWEIGHT EDGE
      return label;
    else if (rawElement != null) {
      if (graph.isUseClassForEdgeLabel()) {
        final String clsName = getRecord().getClassName();
        if (!OrientEdgeType.CLASS_NAME.equals(clsName) && !"OGraphEdge".equals(clsName))
          // RETURN THE CLASS NAME
          return OrientBaseGraph.decodeClassName(clsName);
      }

      graph.setCurrentGraphInThreadLocal();

      final ODocument doc = (ODocument) rawElement.getRecord();
      if (doc == null)
        return null;

      final String label = doc.field(OrientElement.LABEL_FIELD_NAME);
      if (label != null)
        return OrientBaseGraph.decodeClassName(label);
    }
    return null;
  }

  @Override
  public boolean equals(final Object object) {
    if (rawElement == null && object instanceof OrientEdge) {
      final OrientEdge other = (OrientEdge) object;
      return vOut.equals(other.vOut) && vIn.equals(other.vIn)
          && (label == null && other.label == null || label.equals(other.label));
    }
    return super.equals(object);
  }

  @Override
  public Object getId() {
    if (rawElement == null)
      // CREATE A TEMPORARY ID
      return vOut.getIdentity() + "->" + vIn.getIdentity();

    graph.setCurrentGraphInThreadLocal();

    return super.getId();
  }

  @Override
  public <T> T getProperty(final String key) {
    graph.setCurrentGraphInThreadLocal();

    if (rawElement == null)
      // LIGHTWEIGHT EDGE
      return null;

    return super.getProperty(key);
  }

  public boolean isLightweight() {
    return rawElement == null;
  }

  @Override
  public Set<String> getPropertyKeys() {
    if (rawElement == null)
      // LIGHTWEIGHT EDGE
      return Collections.emptySet();

    graph.setCurrentGraphInThreadLocal();

    final Set<String> result = new HashSet<String>();

    for (String field : getRecord().fieldNames())
      if (!field.equals(OrientBaseGraph.CONNECTION_OUT) && !field.equals(OrientBaseGraph.CONNECTION_IN)
          && (graph.isUseClassForEdgeLabel() || !field.equals(OrientElement.LABEL_FIELD_NAME)))
        result.add(field);

    return result;
  }

  @Override
  public void setProperty(final String key, final Object value) {
    graph.setCurrentGraphInThreadLocal();

    if (rawElement == null)
      // LIGHTWEIGHT EDGE
      convertToDocument();

    super.setProperty(key, value);
  }

  @Override
  public <T> T removeProperty(String key) {
    graph.setCurrentGraphInThreadLocal();

    if (rawElement != null)
      // NON LIGHTWEIGHT EDGE
      return super.removeProperty(key);
    return null;
  }

  @Override
  public void remove() {
    checkClass();

    graph.autoStartTransaction();
    for (final Index<? extends Element> index : graph.getManualIndices()) {
      if (Edge.class.isAssignableFrom(index.getIndexClass())) {
        OrientIndex<OrientEdge> idx = (OrientIndex<OrientEdge>) index;
        idx.removeElement(this);
      }
    }

    // OUT VERTEX
    final OIdentifiable inVertexEdge = vIn != null ? vIn : rawElement;
    final ODocument outVertex = getOutVertex().getRecord();

    final String edgeClassName = OrientBaseGraph.encodeClassName(getLabel());

    final boolean useVertexFieldsForEdgeLabels = graph.isUseVertexFieldsForEdgeLabels();

    final String outFieldName = OrientVertex.getConnectionFieldName(Direction.OUT, edgeClassName, useVertexFieldsForEdgeLabels);
    dropEdgeFromVertex(inVertexEdge, outVertex, outFieldName, outVertex.field(outFieldName));

    // IN VERTEX
    final OIdentifiable outVertexEdge = vOut != null ? vOut : rawElement;
    final ODocument inVertex = getInVertex().getRecord();

    final String inFieldName = OrientVertex.getConnectionFieldName(Direction.IN, edgeClassName, useVertexFieldsForEdgeLabels);
    dropEdgeFromVertex(outVertexEdge, inVertex, inFieldName, inVertex.field(inFieldName));

    outVertex.save();
    inVertex.save();

    if (rawElement != null)
      // NON-LIGHTWEIGHT EDGE
      super.remove();
  }

  public final String getBaseClassName() {
    return OrientEdgeType.CLASS_NAME;
  }

  @Override
  public String getElementType() {
    return "Edge";
  }

  public String toString() {
    graph.setCurrentGraphInThreadLocal();

    if (getLabel() == null)
      return StringFactory.E + StringFactory.L_BRACKET + getId() + StringFactory.R_BRACKET + StringFactory.L_BRACKET
          + getVertex(Direction.OUT).getId() + StringFactory.ARROW + getVertex(Direction.IN).getId() + StringFactory.R_BRACKET;

    return StringFactory.edgeString(this);
  }

  public static OIdentifiable getConnection(final ODocument iEdgeRecord, final Direction iDirection) {
    return iEdgeRecord.rawField(iDirection == Direction.OUT ? OrientBaseGraph.CONNECTION_OUT : OrientBaseGraph.CONNECTION_IN);
  }

  /**
   * Returns true if the edge is labeled with any of the passed strings.
   * 
   * @param iLabels
   *          Labels as array of Strings
   * @return
   */
  protected boolean isLabeled(final String[] iLabels) {
    return isLabeled(getLabel(), iLabels);
  }

  /**
   * Returns true if the edge is labeled with any of the passed strings.
   * 
   * @param iLabels
   *          Labels as array of Strings
   * @return
   */
  public static boolean isLabeled(final String iEdgeLabel, final String[] iLabels) {
    if (iLabels != null && iLabels.length > 0) {
      // FILTER LABEL
      if (iEdgeLabel != null)
        for (String l : iLabels)
          if (l.equals(iEdgeLabel))
            // FOUND
            return true;

      // NOT FOUND
      return false;
    }
    // NO LABELS
    return true;
  }

  @Override
  public ODocument getRecord() {
    if (rawElement == null) {
      // CREATE AT THE FLY
      final ODocument tmp = new ODocument(getClassName(label)).setTrackingChanges(false);
      tmp.field("in", vIn);
      tmp.field("out", vOut);
      if (label != null && !graph.isUseClassForEdgeLabel())
        tmp.field("label", label);
      return tmp;
    }

    return super.getRecord();
  }

  public static String getRecordLabel(final OIdentifiable iEdge) {
    if (iEdge == null)
      return null;

    final ODocument edge = iEdge.getRecord();
    if (edge == null)
      return null;

    return edge.field(OrientElement.LABEL_FIELD_NAME);
  }

  public void convertToDocument() {
    if (rawElement != null)
      // ALREADY CONVERTED
      return;

    graph.autoStartTransaction();

    final ODocument vOutRecord = vOut.getRecord();
    final ODocument vInRecord = vIn.getRecord();

    final ODocument doc = createDocument(label);

    doc.field(OrientBaseGraph.CONNECTION_OUT, graph.isKeepInMemoryReferences() ? vOutRecord.getIdentity() : vOutRecord);
    doc.field(OrientBaseGraph.CONNECTION_IN, graph.isKeepInMemoryReferences() ? vInRecord.getIdentity() : vInRecord);
    rawElement = doc;

    final boolean useVertexFieldsForEdgeLabels = graph.isUseVertexFieldsForEdgeLabels();

    final String outFieldName = OrientVertex.getConnectionFieldName(Direction.OUT, label, useVertexFieldsForEdgeLabels);
    OrientVertex.removeEdges(vOutRecord, outFieldName, vInRecord, false, useVertexFieldsForEdgeLabels);

    final String inFieldName = OrientVertex.getConnectionFieldName(Direction.IN, label, useVertexFieldsForEdgeLabels);
    OrientVertex.removeEdges(vInRecord, inFieldName, vOutRecord, false, useVertexFieldsForEdgeLabels);

    // OUT-VERTEX ---> IN-VERTEX/EDGE
    OrientVertex.createLink(vOutRecord, doc, outFieldName);

    // IN-VERTEX ---> OUT-VERTEX/EDGE
    OrientVertex.createLink(vInRecord, doc, inFieldName);

    doc.save();
    vOutRecord.save();
    vInRecord.save();

    vOut = null;
    vIn = null;
    label = null;
  }

  protected ODocument createDocument(final String iLabel) {
    final String className = getClassName(iLabel);

    final ODocument doc = new ODocument(className);

    if (iLabel != null && !graph.isUseClassForEdgeLabel())
      // SET THE LABEL AS FIELD
      doc.field(OrientElement.LABEL_FIELD_NAME, iLabel);

    return doc;
  }

  public String getClassName(final String iLabel) {
    if (iLabel != null && graph.isUseClassForEdgeLabel())
      // USE THE LABEL AS DOCUMENT CLASS
      return checkForClassInSchema(iLabel);

    return OrientEdgeType.CLASS_NAME;
  }

  protected void dropEdgeFromVertex(final OIdentifiable iEdge, final ODocument iVertex, final String iFieldName,
      final Object iFieldValue) {
    if (iFieldValue == null) {
      // NO EDGE? WARN
      OLogManager.instance().debug(this, "Edge not found in vertex's property %s.%s while removing the edge %s",
          iVertex.getIdentity(), iFieldName, iEdge.getIdentity());

    } else if (iFieldValue instanceof OIdentifiable) {
      // FOUND A SINGLE ITEM: JUST REMOVE IT

      if (iFieldValue.equals(iEdge))
        iVertex.removeField(iFieldName);
      else
        // NO EDGE? WARN
        OLogManager.instance().warn(this, "Edge not found in vertex's property %s.%s link while removing the edge %s",
            iVertex.getIdentity(), iFieldName, iEdge.getIdentity());

    } else if (iFieldValue instanceof Collection<?>) {
      // ALREADY A SET: JUST REMOVE THE NEW EDGE
      final Collection<Object> coll = (Collection<Object>) iFieldValue;

      if (!coll.remove(iEdge))
        OLogManager.instance().warn(this, "Edge not found in vertex's property %s.%s set while removing the edge %s",
            iVertex.getIdentity(), iFieldName, iEdge.getIdentity());

      if (coll.size() == 1)
        iVertex.field(iFieldName, coll.iterator().next());
      else if (coll.size() == 0)
        iVertex.removeField(iFieldName);
    } else
      throw new IllegalStateException("Wrong type found in the field '" + iFieldName + "': " + iFieldValue.getClass());
  }
}
