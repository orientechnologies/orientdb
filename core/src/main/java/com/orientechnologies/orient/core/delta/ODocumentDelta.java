/*
 * Copyright 2018 OrientDB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.delta;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.schema.OTypeInterface;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordAbstract;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.ODocumentSerializable;
import com.orientechnologies.orient.core.serialization.OSerializableStream;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.BytesContainer;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.OSerializableWrapper;
import com.orientechnologies.orient.core.storage.OStorage;

import java.io.Serializable;
import java.util.*;

/**
 * @author marko
 */
public class ODocumentDelta implements OIdentifiable {

  protected     Map<String, ValueType>    fields = new HashMap<>();
  private final ODocumentDeltaSerializerI serializer;

  public ODocumentDelta() {
    this.serializer = ODocumentDeltaSerializer.getActiveSerializer();
  }

  public ValueType field(String name) {
    return fields.get(name);
  }

  public void field(String name, ValueType value) {
    fields.put(name, value);
  }

  @Override
  public ORID getIdentity() {
    return field("i").getValue();
  }

  public void setIdentity(ORID identity) {
    ValueType vt = new ValueType(identity, OType.LINK);
    field("i", vt);
  }

  public byte[] serialize() {
    return serializer.toStream(this);
  }

  public void setVersion(int version) {
    ValueType vt = new ValueType(version, OType.INTEGER);
    field("r", vt);
  }

  private Integer getVersion() {
    return field("r").getValue();
  }

  public void deserialize(BytesContainer bytes) {
    serializer.fromStream(this, bytes);
  }

  public boolean isLeaf() {
    return fields.isEmpty();
  }

  public Set<Map.Entry<String, ValueType>> fields() {
    return fields.entrySet();
  }

  private static boolean equalDocsInThisContext(ODocument doc1, ODocument doc2) {

    String[] fieldNames = doc1.fieldNames();

    for (String fieldName : fieldNames) {
      if (!doc2.containsField(fieldName)) {
        return false;
      }
      Object fieldVal = doc1.field(fieldName);
      Object otherFieldVal = doc2.field(fieldName);

      if (!equalVals(fieldVal, otherFieldVal)) {
        return false;
      }
    }

    return true;
  }

  private static boolean equalEmbeddedCollections(Collection<Object> col1, Collection<Object> col2) {
    if (col1.size() != col2.size()) {
      return false;
    }

    if (col1 instanceof List) {
      List<Object> l1 = (List<Object>) col1;
      List<Object> l2 = (List<Object>) col2;
      for (int i = 0; i < l1.size(); i++) {
        Object o1 = l1.get(i);
        Object o2 = l2.get(i);

        if (!equalVals(o1, o2)) {
          return false;
        }
      }
    } else {
      Set<Object> s1 = (Set<Object>) col1;
      //create new set because of remove
      Set<Object> s2 = new HashSet<>(col2);

      for (Object o1 : s1) {
        boolean foundInOther = false;

        Iterator<Object> iter2 = s2.iterator();
        while (iter2.hasNext()) {
          Object o2 = iter2.next();

          if (equalVals(o1, o2)) {
            foundInOther = true;
            iter2.remove();
          }
        }

        if (!foundInOther) {
          return false;
        }
      }

    }
    return true;
  }

  private static boolean equalEmbeddedMaps(Map<String, Object> m1, Map<String, Object> m2) {
    if (m1.size() != m2.size()) {
      return false;
    }

    for (Map.Entry<String, Object> entry : m1.entrySet()) {
      String fieldName = entry.getKey();
      if (!m2.containsKey(fieldName)) {
        return false;
      }

      Object fieldVal = entry.getValue();
      Object otherFieldVal = m2.get(fieldName);

      if (!equalVals(fieldVal, otherFieldVal)) {
        return false;
      }
    }

    return true;
  }

  private static boolean equalVals(Object val1, Object val2) {
    if (val1 == val2) {
      return true;
    }

    if ((val1 == null && val2 != null) || (val1 != null && val2 == null)) {
      return false;
    }

    if (val1 == null && val2 == null) {
      return true;
    }

    OTypeInterface fieldType = ODeltaDocumentFieldType.getFromClass(val1.getClass());
    if (fieldType == OType.LINK && val1 instanceof ODocument) {
      fieldType = OType.EMBEDDED;
    }
    OTypeInterface otherFieldType = ODeltaDocumentFieldType.getFromClass(val2.getClass());
    if (otherFieldType == OType.LINK && val2 instanceof ODocument) {
      otherFieldType = OType.EMBEDDED;
    }

    if (fieldType != otherFieldType) {
      return false;
    }

    if (fieldType == OType.INTEGER || fieldType == OType.LONG || fieldType == OType.SHORT || fieldType == OType.STRING
        || fieldType == OType.DOUBLE || fieldType == OType.FLOAT || fieldType == OType.BYTE || fieldType == OType.BOOLEAN
        || fieldType == OType.DATETIME || fieldType == OType.DATE || fieldType == OType.LINKBAG || fieldType == OType.BINARY
        || fieldType == OType.DECIMAL || fieldType == OType.LINKSET || fieldType == OType.LINKLIST
        || fieldType == ODeltaDocumentFieldType.DELTA_RECORD || fieldType == OType.LINK || fieldType == OType.LINKMAP
        || fieldType == OType.TRANSIENT || fieldType == OType.ANY) {

      if (!Objects.equals(val1, val2)) {
        return false;
      }

    } else if (fieldType == OType.EMBEDDED) {
      ODocument fieldDoc;
      if (val1 instanceof ODocumentSerializable) {
        fieldDoc = ((ODocumentSerializable) val1).toDocument();
      } else {
        fieldDoc = (ODocument) val1;
      }

      ODocument otherFieldDoc;
      if (val2 instanceof ODocumentSerializable) {
        otherFieldDoc = ((ODocumentSerializable) val2).toDocument();
      } else {
        otherFieldDoc = (ODocument) val2;
      }

      if (!equalDocsInThisContext(fieldDoc, otherFieldDoc)) {
        return false;
      }

    } else if (fieldType == OType.EMBEDDEDSET || fieldType == OType.EMBEDDEDLIST) {
      Collection col1;
      if (val1.getClass().isArray())
        col1 = Arrays.asList(OMultiValue.array(val1));
      else
        col1 = (Collection) val1;

      Collection col2;
      if (val2.getClass().isArray())
        col2 = Arrays.asList(OMultiValue.array(val2));
      else
        col2 = (Collection) val2;

      if (!equalEmbeddedCollections(col1, col2)) {
        return false;
      }
    } else if (fieldType == OType.EMBEDDEDMAP) {
      Map m1 = (Map) val1;
      Map m2 = (Map) val2;
      if (!equalEmbeddedMaps(m1, m2)) {
        return false;
      }
    } else if (fieldType == OType.CUSTOM) {
      byte[] v1;
      if (!(val1 instanceof OSerializableStream)) {
        v1 = new OSerializableWrapper((Serializable) val1).toStream();
      } else {
        v1 = ((OSerializableStream) val1).toStream();
      }

      byte[] v2;
      if (!(val2 instanceof OSerializableStream)) {
        v2 = new OSerializableWrapper((Serializable) val2).toStream();
      } else {
        v2 = ((OSerializableStream) val2).toStream();
      }

      if (!Objects.equals(v1, v2)) {
        return false;
      }
    }

    return true;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final ODocumentDelta other = (ODocumentDelta) obj;

    if (fields.size() != other.fields.size()) {
      return false;
    }

    for (Map.Entry<String, ValueType> field : fields.entrySet()) {
      String fieldName = field.getKey();
      if (!other.fields.containsKey(fieldName)) {
        return false;
      }

      Object fieldVal = field.getValue().getValue();
      Object otherFieldVal = other.fields.get(fieldName).getValue();

      if (!equalVals(fieldVal, otherFieldVal)) {
        return false;
      }
    }

    return true;
  }

  @Override
  public int hashCode() {
    int hash = 7;
    hash = 67 * hash + Objects.hashCode(this.fields);
    return hash;
  }

  @Override
  public int compareTo(OIdentifiable iOther) {
    if (iOther == this)
      return 0;

    if (iOther == null)
      return 1;

    final int clusterId = getIdentity().getClusterId();
    final int otherClusterId = iOther.getIdentity().getClusterId();
    if (clusterId == otherClusterId) {
      final long clusterPosition = getIdentity().getClusterPosition();
      final long otherClusterPos = iOther.getIdentity().getClusterPosition();

      return (clusterPosition < otherClusterPos) ? -1 : ((clusterPosition == otherClusterPos) ? 0 : 1);
    } else if (clusterId > otherClusterId)
      return 1;

    return -1;
  }

  @Override
  public int compare(OIdentifiable o1, OIdentifiable o2) {
    return o1.compareTo(o2);
  }

  @Override
  public <T extends ORecord> T getRecord() {
    ODatabaseDocumentInternal database = ODatabaseRecordThreadLocal.instance().get();
    ORecord rec = database.getRecord(getIdentity());
    if (rec instanceof ORecordAbstract) {
      rec = rec.copy();
      ORecordInternal.setVersion(rec, getVersion());
    }
    return (T) rec;
  }

//  public <T extends ORecord> T getRecord(ODatabaseDocumentInternal database) {
//    ORecord rec = database.getRecord(getIdentity());
//    return (T)rec;
//  }

  @Override
  public void lock(boolean iExclusive) {
  }

  @Override
  public boolean isLocked() {
    return false;
  }

  @Override
  public OStorage.LOCKING_STRATEGY lockingStrategy() {
    return OStorage.LOCKING_STRATEGY.NONE;
  }

  @Override
  public void unlock() {
  }
}
