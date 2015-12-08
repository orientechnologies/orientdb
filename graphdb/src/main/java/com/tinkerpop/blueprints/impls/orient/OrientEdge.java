/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientechnologies.com
 *
 */

package com.tinkerpop.blueprints.impls.orient;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Index;
import com.tinkerpop.blueprints.util.ExceptionFactory;
import com.tinkerpop.blueprints.util.StringFactory;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * OrientDB Edge implementation of TinkerPop Blueprints standard. Edges can be classic or lightweight. Lightweight edges have no
 * properties and have no identity on database. Lightweight edges are created by default when an Edge is created without properties.
 * To disable this option execute this command against the database: <code>alter database custom useLightweightEdges=false</code>.
 *
 * @author Luca Garulli (http://www.orientechnologies.com)
 */
@SuppressWarnings("unchecked")
public class OrientEdge extends OrientElement implements Edge {
  private static final long serialVersionUID = 1L;

  protected OIdentifiable vOut;
  protected OIdentifiable vIn;
  protected String        label;

  /**
   * (Internal) Called by serialization
   */
  public OrientEdge() {
    super(null, null);
  }

  protected OrientEdge(final OrientBaseGraph rawGraph, final OIdentifiable rawEdge) {
    super(rawGraph, rawEdge);
  }

  protected OrientEdge(final OrientBaseGraph rawGraph, final OIdentifiable rawEdge, String iLabel) {
    super(rawGraph, rawEdge);
    label = iLabel;
  }

  protected OrientEdge(final OrientBaseGraph rawGraph, final String iLabel, final Object... fields) {
    super(rawGraph, null);
    rawElement = createDocument(iLabel);
    setPropertiesInternal(fields);
  }

  protected OrientEdge(final OrientBaseGraph rawGraph, final OIdentifiable out, final OIdentifiable in) {
    this(rawGraph, out, in, null);
  }

  protected OrientEdge(final OrientBaseGraph rawGraph, final OIdentifiable out, final OIdentifiable in, final String iLabel) {
    super(rawGraph, null);
    vOut = out;
    vIn = in;
    label = iLabel;
  }

  public static OIdentifiable getConnection(final ODocument iEdgeRecord, final Direction iDirection) {
    return iEdgeRecord.rawField(iDirection == Direction.OUT ? OrientBaseGraph.CONNECTION_OUT : OrientBaseGraph.CONNECTION_IN);
  }

  /**
   * (Blueprints Extension) Returns true if the edge is labeled with any of the passed strings.
   *
   * @param iEdgeLabel
   *          Label of current edge
   * @param iLabels
   *          Labels as array of Strings
   * @return true if the edge is labeled with any of the passed strings
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

  /**
   * (Blueprints Extension) Returns the record label if any, otherwise NULL.
   *
   * @param iEdge
   *          Edge instance
   */
  public static String getRecordLabel(final OIdentifiable iEdge) {
    if (iEdge == null)
      return null;

    final ODocument edge = iEdge.getRecord();
    if (edge == null)
      return null;

    return edge.field(OrientElement.LABEL_FIELD_NAME);
  }

  /**
   * (Blueprints Extension) This method does not remove connection from opposite side.
   *
   * @param iVertex
   *          vertex that holds connection
   * @param iFieldName
   *          name of field that holds connection
   * @param iVertexToRemove
   *          target of connection
   */
  private static void removeLightweightConnection(final ODocument iVertex, final String iFieldName,
      final OIdentifiable iVertexToRemove) {
    if (iVertex == null || iVertexToRemove == null)
      return;

    final Object fieldValue = iVertex.field(iFieldName);
    if (fieldValue instanceof OIdentifiable) {
      if (fieldValue.equals(iVertexToRemove)) {
        iVertex.removeField(iFieldName);
      }
    } else if (fieldValue instanceof ORidBag) {
      ((ORidBag) fieldValue).remove(iVertexToRemove);
    }
  }

  public OrientEdgeType getType() {
    final OrientBaseGraph graph = getGraph();
    return isLightweight() ? null : new OrientEdgeType(graph, ((ODocument) rawElement.getRecord()).getSchemaClass());
  }

  /**
   * Returns the connected incoming or outgoing vertex.
   *
   * @param direction
   *          Direction between IN or OUT
   */
  @Override
  public OrientVertex getVertex(final Direction direction) {
    final OrientBaseGraph graph = setCurrentGraphInThreadLocal();

    if (direction.equals(Direction.OUT))
      return new OrientVertex(graph, getOutVertex());
    else if (direction.equals(Direction.IN))
      return new OrientVertex(graph, getInVertex());
    else
      throw ExceptionFactory.bothIsNotSupported();
  }

  /**
   * (Blueprints Extension) Returns the outgoing vertex in form of record.
   */
  public OIdentifiable getOutVertex() {
    if (vOut != null)
      // LIGHTWEIGHT EDGE
      return vOut;

    setCurrentGraphInThreadLocal();

    final ODocument doc = getRecord();
    if (doc == null)
      return null;

    if (settings != null && settings.isKeepInMemoryReferences())
      // AVOID LAZY RESOLVING+SETTING OF RECORD
      return doc.rawField(OrientBaseGraph.CONNECTION_OUT);
    else
      return doc.field(OrientBaseGraph.CONNECTION_OUT);
  }

  /**
   * (Blueprints Extension) Returns the incoming vertex in form of record.
   */
  public OIdentifiable getInVertex() {
    if (vIn != null)
      // LIGHTWEIGHT EDGE
      return vIn;

    setCurrentGraphInThreadLocal();

    final ODocument doc = getRecord();
    if (doc == null)
      return null;

    if (settings != null && settings.isKeepInMemoryReferences())
      // AVOID LAZY RESOLVING+SETTING OF RECORD
      return doc.rawField(OrientBaseGraph.CONNECTION_IN);
    else
      return doc.field(OrientBaseGraph.CONNECTION_IN);
  }

  /**
   * Returns the Edge's label. By default OrientDB binds the Blueprints Label concept to Edge Class. To disable this feature execute
   * this at database level <code>alter database custom useClassForEdgeLabel=false
   * </code>
   */
  @Override
  public String getLabel() {
    if (label != null)
      // LIGHTWEIGHT EDGE
      return label;
    else if (rawElement != null) {
      if (settings != null && settings.isUseClassForEdgeLabel()) {
        final String clsName = getRecord().getClassName();
        if (!OrientEdgeType.CLASS_NAME.equals(clsName) && !"OGraphEdge".equals(clsName))
          // RETURN THE CLASS NAME
          return OrientBaseGraph.decodeClassName(clsName);
      }

      setCurrentGraphInThreadLocal();

      final ODocument doc = rawElement.getRecord();
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
      return vOut.equals(other.vOut) && vIn.equals(other.vIn) && (label != null && label.equals(other.label));
    }
    return super.equals(object);
  }

  /**
   * Returns the Edge Id assuring to save it if it's transient yet.
   */
  @Override
  public Object getId() {
    if (rawElement == null)
      // CREATE A TEMPORARY ID
      return vOut.getIdentity() + "->" + vIn.getIdentity();

    setCurrentGraphInThreadLocal();

    return super.getId();
  }

  /**
   * Returns a Property value.
   *
   * @param key
   *          Property name
   * @return Property value if any, otherwise NULL.
   */
  @Override
  public <T> T getProperty(final String key) {
    setCurrentGraphInThreadLocal();

    if (rawElement == null)
      // LIGHTWEIGHT EDGE
      return null;

    return super.getProperty(key);
  }

  public boolean isLightweight() {
    return rawElement == null;
  }

  /**
   * Returns all the Property names as Set of String. out, in and label are not returned as properties even if are part of the
   * underlying document because are considered internal properties.
   */
  @Override
  public Set<String> getPropertyKeys() {
    if (rawElement == null)
      // LIGHTWEIGHT EDGE
      return Collections.emptySet();

    setCurrentGraphInThreadLocal();

    final Set<String> result = new HashSet<String>();

    for (String field : getRecord().fieldNames())
      if (!field.equals(OrientBaseGraph.CONNECTION_OUT) && !field.equals(OrientBaseGraph.CONNECTION_IN)
          && (settings.isUseClassForEdgeLabel() || !field.equals(OrientElement.LABEL_FIELD_NAME)))
        result.add(field);

    return result;
  }

  /**
   * Set a Property value. If the edge is lightweight, it's transparently transformed into a regular edge.
   *
   * @param key
   *          Property name
   * @param value
   *          Property value
   */
  @Override
  public void setProperty(final String key, final Object value) {
    setCurrentGraphInThreadLocal();

    if (rawElement == null)
      // LIGHTWEIGHT EDGE
      convertToDocument();

    super.setProperty(key, value);
  }

  /**
   * Removed a Property.
   *
   * @param key
   *          Property name
   * @return Old value if any
   */
  @Override
  public <T> T removeProperty(String key) {
    setCurrentGraphInThreadLocal();

    if (rawElement != null)
      // NON LIGHTWEIGHT EDGE
      return super.removeProperty(key);
    return null;
  }

  /**
   * Removes the Edge from the Graph. Connected vertices aren't removed.
   */
  @Override
  public void remove() {
    final OrientBaseGraph graph = getGraph();
    if (!isLightweight())
      checkClass();

    graph.setCurrentGraphInThreadLocal();
    graph.autoStartTransaction();

    for (final Index<? extends Element> index : graph.getIndices()) {
      if (Edge.class.isAssignableFrom(index.getIndexClass())) {
        OrientIndex<OrientEdge> idx = (OrientIndex<OrientEdge>) index;
        idx.removeElement(this);
      }
    }

    if (graph != null)
      graph.removeEdgeInternal(this);
    else
      // IN MEMORY CHANGES ONLY: USE NOTX CLASS
      OrientGraphNoTx.removeEdgeInternal(null, this);
  }

  /**
   * (Blueprints Extension) Returns "E" as base class name all the edge's sub-classes extend.
   */
  public final String getBaseClassName() {
    return OrientEdgeType.CLASS_NAME;
  }

  /**
   * (Blueprints Extension) Returns "Edge".
   */
  @Override
  public String getElementType() {
    return "Edge";
  }

  /**
   * Returns a string representation of the edge.
   */
  public String toString() {
    setCurrentGraphInThreadLocal();

    if (getLabel() == null)
      return StringFactory.E + StringFactory.L_BRACKET + getId() + StringFactory.R_BRACKET + StringFactory.L_BRACKET
          + getVertex(Direction.OUT).getId() + StringFactory.ARROW + getVertex(Direction.IN).getId() + StringFactory.R_BRACKET;

    return StringFactory.edgeString(this);
  }

  /**
   * (Blueprints Extension) Returns the underlying record if it's a regular edge, otherwise it created a document with no identity
   * with the edge properties.
   */
  @Override
  public ODocument getRecord() {
    if (rawElement == null) {
      // CREATE AT THE FLY
      final ODocument tmp = new ODocument(getClassName(label)).setTrackingChanges(false);
      tmp.field(OrientBaseGraph.CONNECTION_IN, vIn.getIdentity());
      tmp.field(OrientBaseGraph.CONNECTION_OUT, vOut.getIdentity());
      if (label != null && settings != null && !settings.isUseClassForEdgeLabel())
        tmp.field(OrientEdge.LABEL_FIELD_NAME, label);
      return tmp;
    }

    return super.getRecord();
  }

  /**
   * (Blueprints Extension) Converts the lightweight edge to a regular edge creating the underlying document to store edge's
   * properties.
   */
  public void convertToDocument() {
    final OrientBaseGraph graph = checkIfAttached();
    if (rawElement != null)
      // ALREADY CONVERTED
      return;

    graph.setCurrentGraphInThreadLocal();
    graph.autoStartTransaction();

    final ODocument vOutRecord = vOut.getRecord();
    final ODocument vInRecord = vIn.getRecord();

    final ODocument doc = createDocument(label);

    doc.field(OrientBaseGraph.CONNECTION_OUT, settings.isKeepInMemoryReferences() ? vOutRecord.getIdentity() : vOutRecord);
    doc.field(OrientBaseGraph.CONNECTION_IN, settings.isKeepInMemoryReferences() ? vInRecord.getIdentity() : vInRecord);
    rawElement = doc;

    final boolean useVertexFieldsForEdgeLabels = settings.isUseVertexFieldsForEdgeLabels();

    final String outFieldName = OrientVertex.getConnectionFieldName(Direction.OUT, label, useVertexFieldsForEdgeLabels);
    removeLightweightConnection(vOutRecord, outFieldName, vInRecord);

    // OUT-VERTEX ---> IN-VERTEX/EDGE
    OrientVertex.createLink(graph, vOutRecord, doc, outFieldName);
    vOutRecord.save();

    final String inFieldName = OrientVertex.getConnectionFieldName(Direction.IN, label, useVertexFieldsForEdgeLabels);
    removeLightweightConnection(vInRecord, inFieldName, vOutRecord);

    // IN-VERTEX ---> OUT-VERTEX/EDGE
    OrientVertex.createLink(graph, vInRecord, doc, inFieldName);
    vInRecord.save();

    vOut = null;
    vIn = null;
    label = null;
  }

  /**
   * (Blueprints Extension) Returns the class name based on graph settings.
   */
  public String getClassName(final String iLabel) {
    if (iLabel != null && (settings == null || settings.isUseClassForEdgeLabel()))
      // USE THE LABEL AS DOCUMENT CLASS
      return checkForClassInSchema(iLabel);

    return OrientEdgeType.CLASS_NAME;
  }

  @Override
  public void writeExternal(final ObjectOutput out) throws IOException {
    super.writeExternal(out);

    out.writeObject(vOut != null ? vOut.getIdentity() : null);
    out.writeObject(vIn != null ? vIn.getIdentity() : null);
    out.writeUTF(label != null ? label : "");
  }

  @Override
  public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
    super.readExternal(in);

    vOut = (OIdentifiable) in.readObject();
    vIn = (OIdentifiable) in.readObject();
    label = in.readUTF();
    if (label.isEmpty())
      label = null;
  }

  /**
   * Returns true if the edge is labeled with any of the passed strings.
   *
   * @param iLabels
   *          Labels as array of Strings
   * @return true if the edge is labeled with any of the passed strings
   */
  protected boolean isLabeled(final String[] iLabels) {
    return isLabeled(getLabel(), iLabels);
  }

  protected ODocument createDocument(final String iLabel) {
    final String className = getClassName(iLabel);

    final ODocument doc = new ODocument(className);

    if (iLabel != null && !settings.isUseClassForEdgeLabel())
      // SET THE LABEL AS FIELD
      doc.field(OrientElement.LABEL_FIELD_NAME, iLabel);

    return doc;
  }

  protected boolean dropEdgeFromVertex(final OIdentifiable iEdge, final ODocument iVertex, final String iFieldName,
      final Object iFieldValue) {
    if (iFieldValue == null) {
      // NO EDGE? WARN
      OLogManager.instance().debug(this, "Edge not found in vertex's property %s.%s while removing the edge %s",
          iVertex.getIdentity(), iFieldName, iEdge.getIdentity());
      return false;

    } else if (iFieldValue instanceof OIdentifiable) {
      // FOUND A SINGLE ITEM: JUST REMOVE IT

      if (iFieldValue.equals(iEdge))
        iVertex.removeField(iFieldName);
      else {
        // NO EDGE? WARN
        OLogManager.instance().warn(this, "Edge not found in vertex's property %s.%s link while removing the edge %s",
            iVertex.getIdentity(), iFieldName, iEdge.getIdentity());
        return false;
      }

    } else if (iFieldValue instanceof ORidBag) {
      // ALREADY A SET: JUST REMOVE THE NEW EDGE
      final ORidBag bag = (ORidBag) iFieldValue;
      bag.remove(iEdge);
    } else if (iFieldValue instanceof Collection<?>) {
      // CONVERT COLLECTION IN TREE-SET AND REMOVE THE EDGE
      final Collection<Object> coll = (Collection<Object>) iFieldValue;

      if (!coll.remove(iEdge)) {
        OLogManager.instance().warn(this, "Edge not found in vertex's property %s.%s set while removing the edge %s",
            iVertex.getIdentity(), iFieldName, iEdge.getIdentity());
        return false;
      }

      if (getGraph().isAutoScaleEdgeType()) {
        // SCALE DOWN THE TYPE (SAVES SPACE BUT COULD CAUSE VALIDATION ERRORS)
        if (coll.size() == 1)
          iVertex.field(iFieldName, coll.iterator().next());
        else if (coll.size() == 0)
          iVertex.removeField(iFieldName);
      }

    } else
      throw new IllegalStateException("Wrong type found in the field '" + iFieldName + "': " + iFieldValue.getClass());

    return true;
  }

}
