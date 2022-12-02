/*
 *
 *  *  Copyright 2016 OrientDB LTD (info(at)orientdb.com)
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
 *  * For more information: http://www.orientdb.com
 *
 */
package com.orientechnologies.orient.core.record.impl;

import com.orientechnologies.common.collection.OMultiCollectionIterator;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.db.record.ORecordLazyList;
import com.orientechnologies.orient.core.db.record.ORecordLazyMultiValue;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ODirection;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.serialization.OSerializableStream;
import com.orientechnologies.orient.core.storage.OStorage;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/** @author Luigi Dell'Aquila */
public class OVertexDelegate implements OVertex {
  private static final String CONNECTION_OUT_PREFIX = "out_";
  private static final String CONNECTION_IN_PREFIX = "in_";
  protected final ODocument element;

  public OVertexDelegate(ODocument entry) {
    this.element = entry;
  }

  @Override
  public Iterable<OEdge> getEdges(ODirection direction) {
    Set<String> prefixes = new HashSet<>();
    switch (direction) {
      case BOTH:
        prefixes.add("in_");
      case OUT:
        prefixes.add("out_");
        break;
      case IN:
        prefixes.add("in_");
    }

    Set<String> candidateClasses = new HashSet<>();
    String[] fieldNames = element.fieldNames();
    for (String fieldName : fieldNames) {
      prefixes.stream()
          .filter(prefix -> fieldName.startsWith(prefix))
          .forEach(
              prefix -> {
                if (fieldName.equals(prefix)) {
                  candidateClasses.add("E");
                } else {
                  candidateClasses.add(fieldName.substring(prefix.length()));
                }
              });
    }
    return getEdges(direction, candidateClasses.toArray(new String[] {}));
  }

  @Override
  public Iterable<OEdge> getEdges(ODirection direction, String... labels) {
    final OMultiCollectionIterator<OEdge> iterable =
        new OMultiCollectionIterator<OEdge>().setEmbedded(true);

    Set<String> fieldNames = null;
    if (labels != null && labels.length > 0) {
      // EDGE LABELS: CREATE FIELD NAME TABLE (FASTER THAN EXTRACT FIELD NAMES FROM THE DOCUMENT)
      fieldNames = getEdgeFieldNames(direction, labels);

      if (fieldNames != null)
        // EARLY FETCH ALL THE FIELDS THAT MATTERS
        element.deserializeFields(fieldNames.toArray(new String[] {}));
    }

    if (fieldNames == null) fieldNames = getPropertyNames();

    for (String fieldName : fieldNames) {

      final OPair<ODirection, String> connection = getConnection(direction, fieldName, labels);
      if (connection == null)
        // SKIP THIS FIELD
        continue;

      Object fieldValue = getProperty(fieldName);

      if (fieldValue != null) {

        if (fieldValue instanceof OIdentifiable) {
          fieldValue = Collections.singleton(fieldValue);
        }
        if (fieldValue instanceof Collection<?>) {
          Collection<?> coll = (Collection<?>) fieldValue;

          // CREATE LAZY Iterable AGAINST COLLECTION FIELD
          if (coll instanceof ORecordLazyMultiValue) {
            iterable.add(
                new OEdgeIterator(
                    this,
                    coll,
                    ((ORecordLazyMultiValue) coll).rawIterator(),
                    connection,
                    labels,
                    coll.size()));
          } else
            iterable.add(new OEdgeIterator(this, coll, coll.iterator(), connection, labels, -1));

        } else if (fieldValue instanceof ORidBag) {
          iterable.add(
              new OEdgeIterator(
                  this,
                  fieldValue,
                  ((ORidBag) fieldValue).rawIterator(),
                  connection,
                  labels,
                  ((ORidBag) fieldValue).size()));
        }
      }
    }

    return iterable;
  }

  @Override
  public Iterable<OEdge> getEdges(ODirection direction, OClass... type) {
    List<String> types = new ArrayList<>();
    if (type != null) {
      for (OClass t : type) {
        types.add(t.getName());
      }
    }
    return getEdges(direction, types.toArray(new String[] {}));
  }

  @Override
  public Iterable<OVertex> getVertices(ODirection direction) {

    return getVertices(direction, (String[]) null);
  }

  @Override
  public Iterable<OVertex> getVertices(ODirection direction, String... type) {
    if (direction == ODirection.BOTH) {
      OMultiCollectionIterator<OVertex> result = new OMultiCollectionIterator<>();
      result.add(getVertices(ODirection.OUT, type));
      result.add(getVertices(ODirection.IN, type));
      return result;
    } else {
      Iterable<OEdge> edges = getEdges(direction, type);
      return new OEdgeToVertexIterable(edges, direction);
    }
  }

  @Override
  public Iterable<OVertex> getVertices(ODirection direction, OClass... type) {
    List<String> types = new ArrayList<>();
    if (type != null) {
      for (OClass t : type) {
        types.add(t.getName());
      }
    }
    return getVertices(direction, types.toArray(new String[] {}));
  }

  @Override
  public OEdge addEdge(OVertex to) {
    return addEdge(to, "E");
  }

  @Override
  public OVertex delete() {
    element.delete();
    return this;
  }

  public static void deleteLinks(OVertex delegate) {
    Iterable<OEdge> allEdges = delegate.getEdges(ODirection.BOTH);
    for (OEdge edge : allEdges) {
      edge.delete();
    }
  }

  protected static void detachOutgointEdge(OVertex vertex, OEdge edge) {
    detachEdge(vertex, edge, "out_");
  }

  protected static void detachIncomingEdge(OVertex vertex, OEdge edge) {
    detachEdge(vertex, edge, "in_");
  }

  protected static void detachEdge(OVertex vertex, OEdge edge, String fieldPrefix) {
    String className = edge.getSchemaType().get().getName();

    if (className.equalsIgnoreCase("e")) {
      className = "";
    }
    String edgeField = fieldPrefix + className;
    Object edgeProp = vertex.getProperty(edgeField);
    OIdentifiable edgeId = null;

    edgeId = ((OIdentifiable) edge).getIdentity();
    if (edgeId == null) {
      // lightweight edge

      Object out = edge.getFrom();
      Object in = edge.getTo();
      if (vertex.getIdentity().equals(out)) {
        edgeId = (OIdentifiable) in;
      } else {
        edgeId = (OIdentifiable) out;
      }
    }

    if (edgeProp instanceof Collection) {
      ((Collection) edgeProp).remove(edgeId);
    } else if (edgeProp instanceof ORidBag) {
      ((ORidBag) edgeProp).remove(edgeId);
    } else if (edgeProp instanceof OIdentifiable
            && ((OIdentifiable) edgeProp).getIdentity() != null
            && ((OIdentifiable) edgeProp).getIdentity().equals(edgeId)
        || edge.isLightweight()) {
      vertex.removeProperty(edgeField);
    } else {
      OLogManager.instance()
          .warn(
              vertex,
              "Error detaching edge: the vertex collection field is of type "
                  + (edgeProp == null ? "null" : edgeProp.getClass()));
    }
  }

  @Override
  public OEdge addEdge(OVertex to, String type) {
    ODatabaseDocument db = getDatabase();
    return db.newEdge(this, to, type == null ? "E" : type);
  }

  @Override
  public OEdge addEdge(OVertex to, OClass type) {
    String className = "E";
    if (type != null) {
      className = type.getName();
    }
    return addEdge(to, className);
  }

  @Override
  public Set<String> getPropertyNames() {
    return element.getPropertyNames();
  }

  @Override
  public <RET> RET getProperty(String name) {
    return element.getProperty(name);
  }

  @Override
  public boolean hasProperty(String propertyName) {
    return element.hasProperty(propertyName);
  }

  @Override
  public void setProperty(String name, Object value) {
    element.setProperty(name, value);
  }

  @Override
  public void setProperty(String name, Object value, OType... fieldType) {
    element.setProperty(name, value, fieldType);
  }

  @Override
  public <RET> RET removeProperty(String name) {
    return element.removeProperty(name);
  }

  @Override
  public Optional<OVertex> asVertex() {
    return Optional.of(this);
  }

  @Override
  public Optional<OEdge> asEdge() {
    return Optional.empty();
  }

  @Override
  public boolean isVertex() {
    return true;
  }

  @Override
  public boolean isEdge() {
    return false;
  }

  @Override
  public Optional<OClass> getSchemaType() {
    return element.getSchemaType();
  }

  @Override
  public <T extends ORecord> T getRecord() {
    return (T) element;
  }

  @Override
  public void lock(boolean iExclusive) {
    element.lock(iExclusive);
  }

  @Override
  public boolean isLocked() {
    return element.isLocked();
  }

  @Override
  public OStorage.LOCKING_STRATEGY lockingStrategy() {
    return element.lockingStrategy();
  }

  @Override
  public void unlock() {
    element.unlock();
  }

  @Override
  public int compareTo(OIdentifiable o) {
    return element.compareTo(o);
  }

  @Override
  public int compare(OIdentifiable o1, OIdentifiable o2) {
    return element.compare(o1, o2);
  }

  protected OPair<ODirection, String> getConnection(
      final ODirection iDirection, final String iFieldName, String... iClassNames) {
    if (iClassNames != null && iClassNames.length == 1 && iClassNames[0].equalsIgnoreCase("E"))
      // DEFAULT CLASS, TREAT IT AS NO CLASS/LABEL
      iClassNames = null;

    OSchema schema =
        ODatabaseRecordThreadLocal.instance().get().getMetadata().getImmutableSchemaSnapshot();

    if (iDirection == ODirection.OUT || iDirection == ODirection.BOTH) {
      // FIELDS THAT STARTS WITH "out_"
      if (iFieldName.startsWith(CONNECTION_OUT_PREFIX)) {
        if (iClassNames == null || iClassNames.length == 0)
          return new OPair<ODirection, String>(
              ODirection.OUT, getConnectionClass(ODirection.OUT, iFieldName));

        // CHECK AGAINST ALL THE CLASS NAMES
        for (String clsName : iClassNames) {

          if (iFieldName.equals(CONNECTION_OUT_PREFIX + clsName))
            return new OPair<ODirection, String>(ODirection.OUT, clsName);

          // GO DOWN THROUGH THE INHERITANCE TREE
          OClass type = schema.getClass(clsName);
          if (type != null) {
            for (OClass subType : type.getAllSubclasses()) {
              clsName = subType.getName();

              if (iFieldName.equals(CONNECTION_OUT_PREFIX + clsName))
                return new OPair<ODirection, String>(ODirection.OUT, clsName);
            }
          }
        }
      }
    }

    if (iDirection == ODirection.IN || iDirection == ODirection.BOTH) {
      // FIELDS THAT STARTS WITH "in_"
      if (iFieldName.startsWith(CONNECTION_IN_PREFIX)) {
        if (iClassNames == null || iClassNames.length == 0)
          return new OPair<ODirection, String>(
              ODirection.IN, getConnectionClass(ODirection.IN, iFieldName));

        // CHECK AGAINST ALL THE CLASS NAMES
        for (String clsName : iClassNames) {

          if (iFieldName.equals(CONNECTION_IN_PREFIX + clsName))
            return new OPair<ODirection, String>(ODirection.IN, clsName);

          // GO DOWN THROUGH THE INHERITANCE TREE
          OClass type = schema.getClass(clsName);
          if (type != null) {
            for (OClass subType : type.getAllSubclasses()) {
              clsName = subType.getName();

              if (iFieldName.equals(CONNECTION_IN_PREFIX + clsName))
                return new OPair<ODirection, String>(ODirection.IN, clsName);
            }
          }
        }
      }
    }

    // NOT FOUND
    return null;
  }

  private String getConnectionClass(final ODirection iDirection, final String iFieldName) {
    if (iDirection == ODirection.OUT) {
      if (iFieldName.length() > CONNECTION_OUT_PREFIX.length())
        return iFieldName.substring(CONNECTION_OUT_PREFIX.length());
    } else if (iDirection == ODirection.IN) {
      if (iFieldName.length() > CONNECTION_IN_PREFIX.length())
        return iFieldName.substring(CONNECTION_IN_PREFIX.length());
    }
    return "E"; // TODO constant
  }

  protected Set<String> getEdgeFieldNames(final ODirection iDirection, String... iClassNames) {
    if (iClassNames != null && iClassNames.length == 1 && iClassNames[0].equalsIgnoreCase("E"))
      // DEFAULT CLASS, TREAT IT AS NO CLASS/LABEL
      iClassNames = null;

    final Set<String> result = new HashSet<>();

    if (iClassNames == null)
    // FALL BACK TO LOAD ALL FIELD NAMES
    {
      return null;
    }
    ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.instance().getIfDefined();
    if (db == null) {
      return null;
    }

    OSchema schema = db.getMetadata().getImmutableSchemaSnapshot();

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
          result.add(CONNECTION_OUT_PREFIX + className);
          break;
        case IN:
          result.add(CONNECTION_IN_PREFIX + className);
          break;
        case BOTH:
          result.add(CONNECTION_OUT_PREFIX + className);
          result.add(CONNECTION_IN_PREFIX + className);
          break;
      }
    }

    return result;
  }

  /** (Internal only) Creates a link between a vertices and a Graph Element. */
  public static Object createLink(
      final ODocument iFromVertex, final OIdentifiable iTo, final String iFieldName) {
    final Object out;
    OType outType = iFromVertex.fieldType(iFieldName);
    Object found = iFromVertex.field(iFieldName);

    final OClass linkClass = ODocumentInternal.getImmutableSchemaClass(iFromVertex);
    if (linkClass == null)
      throw new IllegalArgumentException("Class not found in source vertex: " + iFromVertex);

    final OProperty prop = linkClass.getProperty(iFieldName);
    final OType propType = prop != null && prop.getType() != OType.ANY ? prop.getType() : null;

    if (found == null) {
      if (propType == OType.LINKLIST
          || (prop != null
              && "true".equalsIgnoreCase(prop.getCustom("ordered")))) { // TODO constant
        final Collection coll = new ORecordLazyList(iFromVertex);
        coll.add(iTo);
        out = coll;
        outType = OType.LINKLIST;
      } else if (propType == null || propType == OType.LINKBAG) {
        final ORidBag bag = new ORidBag();
        bag.add(iTo);
        out = bag;
        outType = OType.LINKBAG;
      } else if (propType == OType.LINK) {
        out = iTo;
        outType = OType.LINK;
      } else
        throw new ODatabaseException(
            "Type of field provided in schema '"
                + prop.getType()
                + "' cannot be used for link creation.");

    } else if (found instanceof OIdentifiable) {
      if (prop != null && propType == OType.LINK)
        throw new ODatabaseException(
            "Type of field provided in schema '"
                + prop.getType()
                + "' cannot be used for creation to hold several links.");

      if (prop != null && "true".equalsIgnoreCase(prop.getCustom("ordered"))) { // TODO constant
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

      ((ORidBag) found).add(iTo.getRecord());

    } else if (found instanceof Collection<?>) {
      // USE THE FOUND COLLECTION
      out = null;
      ((Collection<Object>) found).add(iTo);

    } else
      throw new ODatabaseException(
          "Relationship content is invalid on field " + iFieldName + ". Found: " + found);

    if (out != null)
      // OVERWRITE IT
      iFromVertex.setProperty(iFieldName, out, outType);

    return out;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof OIdentifiable)) {
      return false;
    }
    if (!(obj instanceof OElement)) {
      obj = ((OIdentifiable) obj).getRecord();
    }

    return element.equals(((OElement) obj).getRecord());
  }

  @Override
  public int hashCode() {
    return element.hashCode();
  }

  @Override
  public STATUS getInternalStatus() {
    return element.getInternalStatus();
  }

  @Override
  public void setInternalStatus(STATUS iStatus) {
    element.setInternalStatus(iStatus);
  }

  @Override
  public <RET> RET setDirty() {
    element.setDirty();
    return (RET) this;
  }

  @Override
  public void setDirtyNoChanged() {
    element.setDirtyNoChanged();
  }

  @Override
  public ORecordElement getOwner() {
    return element.getOwner();
  }

  @Override
  public byte[] toStream() throws OSerializationException {
    return element.toStream();
  }

  @Override
  public OSerializableStream fromStream(byte[] iStream) throws OSerializationException {
    return element.fromStream(iStream);
  }

  @Override
  public boolean detach() {
    return element.detach();
  }

  @Override
  public <RET extends ORecord> RET reset() {
    element.reset();
    return (RET) this;
  }

  @Override
  public <RET extends ORecord> RET unload() {
    element.unload();
    return (RET) this;
  }

  @Override
  public <RET extends ORecord> RET clear() {
    element.clear();
    return (RET) this;
  }

  @Override
  public <RET extends ORecord> RET copy() {
    return (RET) new OVertexDelegate(element.copy());
  }

  @Override
  public ORID getIdentity() {
    return element.getIdentity();
  }

  @Override
  public int getVersion() {
    return element.getVersion();
  }

  @Override
  public ODatabaseDocument getDatabase() {
    return element.getDatabase();
  }

  @Override
  public boolean isDirty() {
    return element.isDirty();
  }

  @Override
  public <RET extends ORecord> RET load() throws ORecordNotFoundException {
    return (RET) element.load();
  }

  @Override
  public <RET extends ORecord> RET reload() throws ORecordNotFoundException {
    element.reload();
    return (RET) this;
  }

  @Override
  public <RET extends ORecord> RET reload(String fetchPlan, boolean ignoreCache, boolean force)
      throws ORecordNotFoundException {
    element.reload(fetchPlan, ignoreCache, force);
    return (RET) this;
  }

  @Override
  public <RET extends ORecord> RET save() {
    element.save();
    return (RET) this;
  }

  @Override
  public <RET extends ORecord> RET save(String iCluster) {
    element.save(iCluster);
    return (RET) this;
  }

  @Override
  public <RET extends ORecord> RET save(boolean forceCreate) {
    element.save(forceCreate);
    return (RET) this;
  }

  @Override
  public <RET extends ORecord> RET save(String iCluster, boolean forceCreate) {
    element.save(iCluster, forceCreate);
    return (RET) this;
  }

  @Override
  public <RET extends ORecord> RET fromJSON(String iJson) {
    element.fromJSON(iJson);
    return (RET) this;
  }

  @Override
  public String toJSON() {
    return element.toJSON();
  }

  @Override
  public String toJSON(String iFormat) {
    return element.toJSON(iFormat);
  }

  @Override
  public int getSize() {
    return element.getSize();
  }

  public ORID moveTo(final String iClassName, final String iClusterName) {
    return OVertexDocument.moveTo(iClassName, iClusterName, this, (ODatabaseSession) getDatabase());
  }

  public static String getConnectionFieldName(
      final ODirection iDirection,
      final String iClassName,
      final boolean useVertexFieldsForEdgeLabels) {
    if (iDirection == null || iDirection == ODirection.BOTH)
      throw new IllegalArgumentException("Direction not valid");

    if (useVertexFieldsForEdgeLabels) {
      // PREFIX "out_" or "in_" TO THE FIELD NAME
      final String prefix =
          iDirection == ODirection.OUT ? CONNECTION_OUT_PREFIX : CONNECTION_IN_PREFIX;
      if (iClassName == null || iClassName.isEmpty() || iClassName.equals("E")) return prefix;

      return prefix + iClassName;
    } else
      // "out" or "in"
      return iDirection == ODirection.OUT ? "out" : "in";
  }

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
        iVertex.setProperty(iFieldName, iNewVertex);
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

  @Override
  public String toString() {
    if (element != null) {
      return element.toString();
    }
    return super.toString();
  }
}
