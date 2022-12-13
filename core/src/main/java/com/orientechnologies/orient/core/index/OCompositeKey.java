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
package com.orientechnologies.orient.core.index;

import com.orientechnologies.common.comparator.ODefaultComparator;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.ODocumentSerializable;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetworkV37;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Container for the list of heterogeneous values that are going to be stored in in index as
 * composite keys.
 *
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com), Artem Orobets
 */
public class OCompositeKey
    implements Comparable<OCompositeKey>, Serializable, ODocumentSerializable {
  private static final long serialVersionUID = 1L;
  /** */
  private final List<Object> keys;

  public OCompositeKey(final List<?> keys) {
    this.keys = new ArrayList<>(keys.size());

    for (final Object key : keys) {
      addKey(key);
    }
  }

  public OCompositeKey(final Object... keys) {
    this.keys = new ArrayList<>(keys.length);

    for (final Object key : keys) {
      addKey(key);
    }
  }

  public OCompositeKey() {
    this.keys = new ArrayList<>();
  }

  public OCompositeKey(final int size) {
    this.keys = new ArrayList<>(size);
  }

  /** Clears the keys array for reuse of the object */
  public void reset() {
    if (this.keys != null) {
      this.keys.clear();
    }
  }

  /** */
  public List<Object> getKeys() {
    return Collections.unmodifiableList(keys);
  }

  /**
   * Add new key value to the list of already registered values.
   *
   * <p>If passed in value is {@link OCompositeKey} itself then its values will be copied in current
   * index. But key itself will not be added.
   *
   * @param key Key to add.
   */
  public void addKey(final Object key) {
    if (key instanceof OCompositeKey) {
      final OCompositeKey compositeKey = (OCompositeKey) key;
      for (final Object inKey : compositeKey.keys) {
        addKey(inKey);
      }
    } else {
      keys.add(key);
    }
  }

  /**
   * Performs partial comparison of two composite keys.
   *
   * <p>Two objects will be equal if the common subset of their keys is equal. For example if first
   * object contains two keys and second contains four keys then only first two keys will be
   * compared.
   *
   * @param otherKey Key to compare.
   * @return a negative integer, zero, or a positive integer as this object is less than, equal to,
   *     or greater than the specified object.
   */
  public int compareTo(final OCompositeKey otherKey) {
    final Iterator<Object> inIter = keys.iterator();
    final Iterator<Object> outIter = otherKey.keys.iterator();

    while (inIter.hasNext() && outIter.hasNext()) {
      final Object inKey = inIter.next();
      final Object outKey = outIter.next();

      if (outKey instanceof OAlwaysGreaterKey) {
        return -1;
      }

      if (outKey instanceof OAlwaysLessKey) {
        return 1;
      }

      if (inKey instanceof OAlwaysGreaterKey) {
        return 1;
      }

      if (inKey instanceof OAlwaysLessKey) {
        return -1;
      }

      final int result = ODefaultComparator.INSTANCE.compare(inKey, outKey);
      if (result != 0) {
        return result;
      }
    }
    return 0;
  }

  /** {@inheritDoc } */
  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    final OCompositeKey that = (OCompositeKey) o;

    return keys.equals(that.keys);
  }

  /** {@inheritDoc } */
  @Override
  public int hashCode() {
    return keys.hashCode();
  }

  /** {@inheritDoc } */
  @Override
  public String toString() {
    return "OCompositeKey{" + "keys=" + keys + '}';
  }

  @Override
  public ODocument toDocument() {
    final ODocument document = new ODocument();
    for (int i = 0; i < keys.size(); i++) {
      document.field("key" + i, keys.get(i));
    }

    return document;
  }

  @Override
  public void fromDocument(ODocument document) {
    document.setLazyLoad(false);

    final String[] fieldNames = document.fieldNames();

    final SortedMap<Integer, Object> keyMap = new TreeMap<>();

    for (String fieldName : fieldNames) {
      if (fieldName.startsWith("key")) {
        final String keyIndex = fieldName.substring(3);
        keyMap.put(Integer.valueOf(keyIndex), document.field(fieldName));
      }
    }

    keys.clear();
    keys.addAll(keyMap.values());
  }

  // Alternative (de)serialization methods that avoid converting the OCompositeKey to a document.
  public void toStream(ORecordSerializerNetworkV37 serializer, DataOutput out) throws IOException {
    int l = keys.size();
    out.writeInt(l);
    for (Object key : keys) {
      if (key instanceof OCompositeKey) {
        throw new OSerializationException("Cannot serialize unflattened nested composite key.");
      }
      if (key == null) {
        out.writeByte((byte) -1);
      } else {
        OType type = OType.getTypeByValue(key);
        byte[] bytes = serializer.serializeValue(key, type);
        out.writeByte((byte) type.getId());
        out.writeInt(bytes.length);
        out.write(bytes);
      }
    }
  }

  public void fromStream(ORecordSerializerNetworkV37 serializer, DataInput in) throws IOException {
    int l = in.readInt();
    for (int i = 0; i < l; i++) {
      byte b = in.readByte();
      if (b == -1) {
        addKey(null);
      } else {
        int len = in.readInt();
        byte[] bytes = new byte[len];
        in.readFully(bytes);
        OType type = OType.getById(b);
        Object k = serializer.deserializeValue(bytes, type);
        addKey(k);
      }
    }
  }
}
