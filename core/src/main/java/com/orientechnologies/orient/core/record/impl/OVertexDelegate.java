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
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordLazyMultiValue;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.record.ODirection;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.storage.OStorage;

import java.util.*;

/**
 * @author Luigi Dell'Aquila
 */
public class OVertexDelegate implements OVertex {
  private static final String CONNECTION_OUT_PREFIX = "out_";
  private static final String CONNECTION_IN_PREFIX  = "in_";
  private final ODocument element;

  public OVertexDelegate(ODocument entry) {
    this.element = entry;
  }

  @Override public Iterable<OEdge> getEdges(ODirection direction) {
    return null;//TODO
  }

  @Override public Iterable<OEdge> getEdges(ODirection direction, String... labels) {
    final OMultiCollectionIterator<OEdge> iterable = new OMultiCollectionIterator<OEdge>().setEmbedded(true);

    Set<String> fieldNames = null;
    if (labels != null && labels.length > 0) {
      // EDGE LABELS: CREATE FIELD NAME TABLE (FASTER THAN EXTRACT FIELD NAMES FROM THE DOCUMENT)
      fieldNames = getEdgeFieldNames(direction, labels);

      if (fieldNames != null)
        // EARLY FETCH ALL THE FIELDS THAT MATTERS
        element.deserializeFields(fieldNames.toArray(new String[] {}));
    }

    if (fieldNames == null)
      fieldNames = getPropertyNames();

    for (String fieldName : fieldNames) {

      final OPair<ODirection, String> connection = getConnection(direction, fieldName, labels);
      if (connection == null)
        // SKIP THIS FIELD
        continue;

      Object fieldValue = getProperty(fieldName);

      if (fieldValue != null) {
        final OIdentifiable destinationVId = null;

        if (fieldValue instanceof OIdentifiable) {
          fieldValue = Collections.singleton(fieldValue);
        }
        if (fieldValue instanceof Collection<?>) {
          Collection<?> coll = (Collection<?>) fieldValue;

          // CREATE LAZY Iterable AGAINST COLLECTION FIELD
          if (coll instanceof ORecordLazyMultiValue) {
            iterable
                .add(new OEdgeIterator(this, coll, ((ORecordLazyMultiValue) coll).rawIterator(), connection, labels, coll.size()));
          } else
            iterable.add(new OEdgeIterator(this, coll, coll.iterator(), connection, labels, -1));

        } else if (fieldValue instanceof ORidBag) {
          iterable.add(new OEdgeIterator(this, fieldValue, ((ORidBag) fieldValue).rawIterator(), connection, labels,
              ((ORidBag) fieldValue).size()));
        }
      }
    }

    return iterable;
  }

  @Override public Iterable<OEdge> getEdges(ODirection direction, OClass... type) {
    List<String> types = new ArrayList<>();
    if (type != null) {
      for (OClass t : type) {
        types.add(t.getName());
      }
    }
    return getEdges(direction, types.toArray(new String[] {}));

  }

  @Override public Iterable<OVertex> getVertices(ODirection direction) {

    return getVertices(direction, (String[]) null);
  }

  @Override public Iterable<OVertex> getVertices(ODirection direction, String... type) {
    return null;//TODO
  }

  @Override public Iterable<OVertex> getVertices(ODirection direction, OClass... type) {
    List<String> types = new ArrayList<>();
    if (type != null) {
      for (OClass t : type) {
        types.add(t.getName());
      }
    }
    return getVertices(direction, types.toArray(new String[] {}));

  }

  @Override public OEdge addEdge(OVertex to) {
    return null;//TODO
  }

  @Override public OEdge addEdge(OVertex to, String type) {
    return null;//TODO
  }

  @Override public OEdge addEdge(OVertex to, OClass type) {
    return null;//TODO
  }

  @Override public Set<String> getPropertyNames() {
    return element.getPropertyNames();
  }

  @Override public <RET> RET getProperty(String name) {
    return element.getProperty(name);
  }

  @Override public void setProperty(String name, Object value) {
    element.setProperty(name, value);
  }

  @Override public Optional<OVertex> asVertex() {
    return Optional.of(this);
  }

  @Override public Optional<OEdge> asEdge() {
    return Optional.empty();
  }

  @Override public boolean isDocument() {
    return true;
  }

  @Override public boolean isVertex() {
    return true;
  }

  @Override public boolean isEdge() {
    return false;
  }

  @Override public Optional<OClass> getType() {
    return Optional.ofNullable(element.getSchemaClass());
  }

  @Override public ORID getIdentity() {
    return element.getIdentity();
  }

  @Override public <T extends ORecord> T getRecord() {
    return (T) element;
  }

  @Override public void lock(boolean iExclusive) {
    element.lock(iExclusive);
  }

  @Override public boolean isLocked() {
    return element.isLocked();
  }

  @Override public OStorage.LOCKING_STRATEGY lockingStrategy() {
    return element.lockingStrategy();
  }

  @Override public void unlock() {
    element.unlock();
  }

  @Override public int compareTo(OIdentifiable o) {
    return element.compareTo(o);
  }

  @Override public int compare(OIdentifiable o1, OIdentifiable o2) {
    return element.compare(o1, o2);
  }

  protected OPair<ODirection, String> getConnection(final ODirection iDirection, final String iFieldName, String... iClassNames) {
    if (iClassNames != null && iClassNames.length == 1 && iClassNames[0].equalsIgnoreCase("E"))
      // DEFAULT CLASS, TREAT IT AS NO CLASS/LABEL
      iClassNames = null;

    OSchema schema = ODatabaseRecordThreadLocal.INSTANCE.get().getMetadata().getSchema();

    if (iDirection == ODirection.OUT || iDirection == ODirection.BOTH) {
      // FIELDS THAT STARTS WITH "out_"
      if (iFieldName.startsWith(CONNECTION_OUT_PREFIX)) {
        if (iClassNames == null || iClassNames.length == 0)
          return new OPair<ODirection, String>(ODirection.OUT, getConnectionClass(ODirection.OUT, iFieldName));

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
          return new OPair<ODirection, String>(ODirection.IN, getConnectionClass(ODirection.IN, iFieldName));

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
    return "E";//TODO constant
  }

  private boolean isSubclassOfAny(OClass clazz, String[] labels) {
    for (String s : labels) {
      if (clazz.isSubClassOf(s)) {
        return true;
      }
    }
    return false;
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
    ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();
    if (db == null) {
      return null;
    }

    OSchema schema = db.getMetadata().getSchema();

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
}
