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
package com.orientechnologies.orient.core.type.tree.provider;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OBooleanSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.OMemoryStream;
import com.orientechnologies.orient.core.serialization.OSerializableStream;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OLinkSerializer;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializer;

import java.io.IOException;
import java.util.Arrays;

@SuppressWarnings("unchecked")
public class OMVRBTreeMapEntryProvider<K, V> extends OMVRBTreeEntryDataProviderAbstract<K, V> {
  /**
   * Current version of serialization format for single MVRBTree node. Versions have negative numbers for backward compatibility
   * with previous format that does not have version number, but first value in serialized content was non-negative integer.
   */
  private static final int            CURRENT_VERSION  = -1;
  private static final OProfiler PROFILER         = Orient.instance().getProfiler();

  private static final long           serialVersionUID = 1L;
  protected K[]                       keys;
  protected V[]                       values;
  protected int[]                     serializedKeys;
  protected int[]                     serializedValues;

  private byte[]                      buffer;

  public OMVRBTreeMapEntryProvider(final OMVRBTreeMapProvider<K, V> iTreeDataProvider) {
    super(iTreeDataProvider, OMemoryStream.DEF_SIZE);
    keys = (K[]) new Object[pageSize];
    values = (V[]) new Object[pageSize];
    serializedKeys = new int[pageSize];
    serializedValues = new int[pageSize];
  }

  public OMVRBTreeMapEntryProvider(final OMVRBTreeMapProvider<K, V> iTreeDataProvider, final ORID iRID) {
    super(iTreeDataProvider, iRID);
  }

  public K getKeyAt(final int iIndex) {
    K k = keys[iIndex];
    if (k == null)
      try {
        PROFILER.updateCounter(PROFILER.getProcessMetric("mvrbtree.entry.unserializeKey"), "Deserialize a MVRBTree entry key", 1);

        k = (K) keyFromStream(iIndex);

        if (iIndex == 0 || iIndex == size || ((OMVRBTreeMapProvider<K, V>) treeDataProvider).keepKeysInMemory)
          // KEEP THE UNMARSHALLED KEY IN MEMORY. TO OPTIMIZE FIRST AND LAST ITEM ARE ALWAYS KEPT IN MEMORY TO SPEEDUP FREQUENT
          // NODE CHECKING OF BOUNDS
          keys[iIndex] = k;

      } catch (IOException e) {
        OLogManager.instance().error(this, "Cannot lazy load the key #" + iIndex + " in tree node " + this, e,
            OSerializationException.class);
      }

    return k;
  }

  public V getValueAt(final int iIndex) {
    V v = values[iIndex];
    if (v == null)
      try {
        PROFILER.updateCounter(PROFILER.getProcessMetric("mvrbtree.entry.unserializeValue"), "Deserialize a MVRBTree entry value",
            1);

        v = (V) valueFromStream(iIndex);

        if (((OMVRBTreeMapProvider<K, V>) treeDataProvider).keepValuesInMemory)
          // KEEP THE UNMARSHALLED VALUE IN MEMORY
          values[iIndex] = v;

      } catch (IOException e) {

        OLogManager.instance().error(this, "Cannot lazy load the value #" + iIndex + " in tree node " + this, e,
            OSerializationException.class);
      }

    return v;
  }

  public boolean setValueAt(int iIndex, final V iValue) {
    values[iIndex] = iValue;
    serializedValues[iIndex] = 0;
    return setDirty();
  }

  public boolean insertAt(final int iIndex, final K iKey, final V iValue) {
    if (iIndex < size) {
      // MOVE RIGHT TO MAKE ROOM FOR THE ITEM
      System.arraycopy(keys, iIndex, keys, iIndex + 1, size - iIndex);
      System.arraycopy(values, iIndex, values, iIndex + 1, size - iIndex);
      System.arraycopy(serializedKeys, iIndex, serializedKeys, iIndex + 1, size - iIndex);
      System.arraycopy(serializedValues, iIndex, serializedValues, iIndex + 1, size - iIndex);
    }

    keys[iIndex] = iKey;
    values[iIndex] = iValue;
    serializedKeys[iIndex] = 0;
    serializedValues[iIndex] = 0;
    size++;

    return setDirty();
  }

  public boolean removeAt(final int iIndex) {
    if (iIndex == size - 1) {
      // LAST ONE: JUST REMOVE IT
    } else if (iIndex > -1) {
      // SHIFT LEFT THE VALUES
      System.arraycopy(keys, iIndex + 1, keys, iIndex, size - iIndex - 1);
      System.arraycopy(values, iIndex + 1, values, iIndex, size - iIndex - 1);
      System.arraycopy(serializedKeys, iIndex + 1, serializedKeys, iIndex, size - iIndex - 1);
      System.arraycopy(serializedValues, iIndex + 1, serializedValues, iIndex, size - iIndex - 1);
    }

    // FREE RESOURCES
    size--;
    serializedKeys[size] = 0;
    serializedValues[size] = 0;
    keys[size] = null;
    values[size] = null;
    return setDirty();
  }

  /**
   * @TODO Optimize by copying only real data and not the entire source buffer.
   */
  public boolean copyDataFrom(final OMVRBTreeEntryDataProvider<K, V> iFrom, final int iStartPosition) {
    final OMVRBTreeMapEntryProvider<K, V> parent = (OMVRBTreeMapEntryProvider<K, V>) iFrom;
    size = iFrom.getSize() - iStartPosition;
    System.arraycopy(parent.serializedKeys, iStartPosition, serializedKeys, 0, size);
    System.arraycopy(parent.serializedValues, iStartPosition, serializedValues, 0, size);
    System.arraycopy(parent.keys, iStartPosition, keys, 0, size);
    System.arraycopy(parent.values, iStartPosition, values, 0, size);

    if (buffer == null && parent.buffer == null) {
      stream = null;
      return setDirty();
    }

    if (parent.buffer == null) {
      buffer = null;
      stream = null;
      return setDirty();
    }

    if (buffer == null || buffer.length < parent.buffer.length)
      buffer = new byte[parent.buffer.length];

    System.arraycopy(parent.buffer, 0, buffer, 0, parent.buffer.length);

    if (buffer == null)
      stream = null;
    else {
      if (stream != null)
        stream.setSource(buffer);
      else
        stream = new OMemoryStream(buffer);
    }

    return setDirty();
  }

  public boolean truncate(final int iNewSize) {
    // TRUNCATE PARENT
    Arrays.fill(serializedKeys, iNewSize, pageSize, 0);
    Arrays.fill(serializedValues, iNewSize, pageSize, 0);
    Arrays.fill(keys, iNewSize, size, null);
    Arrays.fill(values, iNewSize, size, null);
    size = iNewSize;
    return setDirty();
  }

  public boolean copyFrom(final OMVRBTreeEntryDataProvider<K, V> iSource) {
    final OMVRBTreeMapEntryProvider<K, V> source = (OMVRBTreeMapEntryProvider<K, V>) iSource;

    serializedKeys = new int[source.serializedKeys.length];
    System.arraycopy(source.serializedKeys, 0, serializedKeys, 0, source.serializedKeys.length);

    serializedValues = new int[source.serializedValues.length];
    System.arraycopy(source.serializedValues, 0, serializedValues, 0, source.serializedValues.length);

    keys = (K[]) new Object[source.keys.length];
    System.arraycopy(source.keys, 0, keys, 0, source.keys.length);

    values = (V[]) new Object[source.values.length];
    System.arraycopy(source.values, 0, values, 0, source.values.length);

    size = source.size;

    if (buffer == null && source.buffer == null) {
      stream = null;
      return setDirty();
    }

    if (source.buffer == null) {
      buffer = null;
      stream = null;
      return setDirty();
    }

    if (buffer == null || buffer.length < source.buffer.length)
      buffer = new byte[source.buffer.length];

    System.arraycopy(source.buffer, 0, buffer, 0, source.buffer.length);

    if (buffer == null)
      stream = null;
    else {
      if (stream != null)
        stream.setSource(buffer);
      else
        stream = new OMemoryStream(buffer);
    }

    return setDirty();
  }

  @Override
  public void delete() {
    super.delete();
    // FORCE REMOVING OF K/V AND SEIALIZED K/V AS WELL
    keys = null;
    values = null;
    serializedKeys = null;
    serializedValues = null;
  }

  @Override
  public void clear() {
    super.clear();

    buffer = null;
    keys = null;
    values = null;
    serializedKeys = null;
    serializedValues = null;
  }

  public OSerializableStream fromStream(byte[] iStream) throws OSerializationException {
    final long timer = PROFILER.startChrono();
    try {
      // @COMPATIBILITY BEFORE 1.0
      if (OIntegerSerializer.INSTANCE.deserializeLiteral(iStream, 0) >= 0) {
        OLogManager.instance().warn(
            this,
            "Previous version of serialization format was found for node with id " + record.getIdentity()
                + " conversion to new format will be performed."
                + " It will take some time. If this message is shown constantly please recreate indexes.");

        iStream = convertIntoNewSerializationFormat(iStream);

        OLogManager.instance().warn(this,
            "Conversion of data to new serialization format for node " + record.getIdentity() + " was finished. ");

      }

      if (((OMVRBTreeMapProvider<K, V>) treeDataProvider).valueSerializer instanceof OBinarySerializer)
        fromStreamUsingBinarySerializer(iStream);
      else
        fromStreamUsingBinaryStreamSerializer(iStream);
      return this;
    } catch (IOException e) {
      throw new OSerializationException("Can not unmarshall tree node with id ", e);
    } finally {
      PROFILER.stopChrono(PROFILER.getProcessMetric("mvrbtree.entry.fromStream"), "Deserialize a MVRBTree entry", timer);
    }
  }

  public byte[] toStream() throws OSerializationException {
    final long timer = PROFILER.startChrono();
    try {
      if (((OMVRBTreeMapProvider<K, V>) treeDataProvider).valueSerializer instanceof OBinarySerializer)
        toStreamUsingBinarySerializer();
      else
        toStreamUsingBinaryStreamSerializer();

      record.fromStream(buffer);
      return buffer;

    } catch (IOException e) {
      throw new OSerializationException("Cannot marshall RB+Tree node", e);
    } finally {
      PROFILER.stopChrono(PROFILER.getProcessMetric("mvrbtree.entry.toStream"), "Serialize a MVRBTree entry", timer);
    }
  }

  private void toStreamUsingBinarySerializer() {
    int bufferSize = 2 * OIntegerSerializer.INT_SIZE;

    bufferSize += OLinkSerializer.INSTANCE.getObjectSize(parentRid) * 3;
    bufferSize += OBooleanSerializer.BOOLEAN_SIZE;
    bufferSize += OIntegerSerializer.INT_SIZE;

    for (int i = 0; i < size; ++i)
      bufferSize += getKeySize(i);

    for (int i = 0; i < size; ++i)
      bufferSize += getBinaryValueSize(i);

    byte[] outBuffer = new byte[bufferSize];

    int offset = serializeMetadata(outBuffer, size, pageSize, parentRid, leftRid, rightRid, color);

    for (int i = 0; i < size; i++) {
      offset = serializeKey(outBuffer, offset, i);
    }

    for (int i = 0; i < size; i++) {
      offset = serializeBinaryValue(outBuffer, offset, i);
    }

    buffer = outBuffer;
  }

  private int serializeMetadata(byte[] newBuffer, int iSize, int iPageSize, ORID iParentId, ORID iLeftRid, ORID iRightRid,
      boolean iColor) {
    int offset = 0;

    OIntegerSerializer.INSTANCE.serializeLiteral(CURRENT_VERSION, newBuffer, offset);
    offset += OIntegerSerializer.INT_SIZE;

    OIntegerSerializer.INSTANCE.serializeLiteral(iPageSize, newBuffer, offset);
    offset += OIntegerSerializer.INT_SIZE;

    OLinkSerializer.INSTANCE.serialize(iParentId, newBuffer, offset);
    offset += OLinkSerializer.INSTANCE.getObjectSize(iParentId);

    OLinkSerializer.INSTANCE.serialize(iLeftRid, newBuffer, offset);
    offset += OLinkSerializer.INSTANCE.getObjectSize(iLeftRid);

    OLinkSerializer.INSTANCE.serialize(iRightRid, newBuffer, offset);
    offset += OLinkSerializer.INSTANCE.getObjectSize(iRightRid);

    OBooleanSerializer.INSTANCE.serializeLiteral(iColor, newBuffer, offset);
    offset += OBooleanSerializer.BOOLEAN_SIZE;

    OIntegerSerializer.INSTANCE.serializeLiteral(iSize, newBuffer, offset);
    offset += OIntegerSerializer.INT_SIZE;
    return offset;
  }

  private int deserializeMetadata(byte[] inBuffer) {
    int offset = 0;

    int currentVersion = OIntegerSerializer.INSTANCE.deserializeLiteral(inBuffer, offset);
    offset += OIntegerSerializer.INT_SIZE;

    if (currentVersion != CURRENT_VERSION)
      throw new OSerializationException("MVRBTree node is stored using " + currentVersion
          + " version of serialization format but current version is " + CURRENT_VERSION + ".");

    pageSize = OIntegerSerializer.INSTANCE.deserializeLiteral(inBuffer, offset);
    offset += OIntegerSerializer.INT_SIZE;

    parentRid = OLinkSerializer.INSTANCE.deserialize(inBuffer, offset);
    offset += OLinkSerializer.INSTANCE.getObjectSize(parentRid);

    leftRid = OLinkSerializer.INSTANCE.deserialize(inBuffer, offset);
    offset += OLinkSerializer.INSTANCE.getObjectSize(leftRid);

    rightRid = OLinkSerializer.INSTANCE.deserialize(inBuffer, offset);
    offset += OLinkSerializer.INSTANCE.getObjectSize(rightRid);

    color = OBooleanSerializer.INSTANCE.deserializeLiteral(inBuffer, offset);
    offset += OBooleanSerializer.BOOLEAN_SIZE;

    size = OIntegerSerializer.INSTANCE.deserializeLiteral(inBuffer, offset);
    offset += OIntegerSerializer.INT_SIZE;

    if (size > pageSize)
      throw new OConfigurationException("Loaded index with page size set to " + pageSize + " while the loaded was built with: "
          + size);

    return offset;
  }

  private int serializeBinaryValue(byte[] newBuffer, int offset, int i) {
    final OBinarySerializer<V> valueSerializer = (OBinarySerializer<V>) ((OMVRBTreeMapProvider<K, V>) treeDataProvider).valueSerializer;

    if (serializedValues[i] <= 0) {
      PROFILER.updateCounter(PROFILER.getProcessMetric("mvrbtree.entry.serializeValue"), "Serialize a MVRBTree entry value", 1);
      valueSerializer.serialize(values[i], newBuffer, offset);
      offset += valueSerializer.getObjectSize(values[i]);
    } else {
      final int size = valueSerializer.getObjectSize(buffer, serializedValues[i]);
      System.arraycopy(buffer, serializedValues[i], newBuffer, offset, size);
      serializedValues[i] = offset;
      offset += size;
    }
    return offset;
  }

  private int serializeKey(byte[] newBuffer, int offset, int i) {
    final OBinarySerializer<K> keySerializer = ((OMVRBTreeMapProvider<K, V>) treeDataProvider).keySerializer;
    if (serializedKeys[i] <= 0) {
      PROFILER.updateCounter(PROFILER.getProcessMetric("mvrbtree.entry.serializeKey"), "Serialize a MVRBTree entry key", 1);
      keySerializer.serialize(keys[i], newBuffer, offset);
      offset += keySerializer.getObjectSize(keys[i]);
    } else {
      final int size = keySerializer.getObjectSize(buffer, serializedKeys[i]);
      System.arraycopy(buffer, serializedKeys[i], newBuffer, offset, size);
      serializedKeys[i] = offset;
      offset += size;
    }
    return offset;
  }

  private int getKeySize(final int iIndex) {
    final OBinarySerializer<K> serializer = ((OMVRBTreeMapProvider<K, V>) treeDataProvider).keySerializer;
    if (serializedKeys[iIndex] <= 0)
      return serializer.getObjectSize(keys[iIndex]);

    return serializer.getObjectSize(buffer, serializedKeys[iIndex]);
  }

  private int getBinaryValueSize(final int iIndex) {
    final OBinarySerializer<V> serializer = (OBinarySerializer<V>) ((OMVRBTreeMapProvider<K, V>) treeDataProvider).valueSerializer;

    if (serializedValues[iIndex] <= 0)
      return serializer.getObjectSize(values[iIndex]);

    return serializer.getObjectSize(buffer, serializedValues[iIndex]);
  }

  private void toStreamUsingBinaryStreamSerializer() throws IOException {
    int bufferSize = 2 * OIntegerSerializer.INT_SIZE;

    bufferSize += OLinkSerializer.INSTANCE.getObjectSize(parentRid) * 3;
    bufferSize += OBooleanSerializer.BOOLEAN_SIZE;
    bufferSize += OIntegerSerializer.INT_SIZE;

    for (int i = 0; i < size; ++i)
      bufferSize += getKeySize(i);

    final byte[] outBuffer = new byte[bufferSize * 2];

    int offset = serializeMetadata(outBuffer, size, pageSize, parentRid, leftRid, rightRid, color);

    for (int i = 0; i < size; i++) {
      offset = serializeKey(outBuffer, offset, i);
    }

    final OMemoryStream outStream = new OMemoryStream(outBuffer);
    try {
      outStream.jump(offset);

      for (int i = 0; i < size; ++i)
        serializedValues[i] = outStream.set(serializeStreamValue(i));

      buffer = outStream.toByteArray();
    } finally {
      outStream.close();
    }

    if (stream == null)
      stream = new OMemoryStream(buffer);
    else
      stream.setSource(buffer);
  }

  private void fromStreamUsingBinarySerializer(final byte[] inBuffer) {
    int offset = deserializeMetadata(inBuffer);

    serializedKeys = new int[pageSize];
    keys = (K[]) new Object[pageSize];

    final OBinarySerializer<K> keySerializer = ((OMVRBTreeMapProvider<K, V>) treeDataProvider).keySerializer;
    if (keySerializer == null)
      throw new IllegalStateException("key serializer wasn't found");
    for (int i = 0; i < size; i++) {
      serializedKeys[i] = offset;
      offset += keySerializer.getObjectSize(inBuffer, offset);
    }

    serializedValues = new int[pageSize];
    values = (V[]) new Object[pageSize];

    final OBinarySerializer<V> valueSerializer = (OBinarySerializer<V>) ((OMVRBTreeMapProvider<K, V>) treeDataProvider).valueSerializer;
    if (valueSerializer == null)
      throw new IllegalStateException("value serializer wasn't found");

    for (int i = 0; i < size; i++) {
      serializedValues[i] = offset;
      offset += valueSerializer.getObjectSize(inBuffer, offset);
    }

    buffer = inBuffer;
  }

  private void fromStreamUsingBinaryStreamSerializer(final byte[] inBuffer) {
    int offset = deserializeMetadata(inBuffer);

    serializedKeys = new int[pageSize];
    keys = (K[]) new Object[pageSize];

    final OBinarySerializer<K> keySerializer = ((OMVRBTreeMapProvider<K, V>) treeDataProvider).keySerializer;
    for (int i = 0; i < size; i++) {
      serializedKeys[i] = offset;
      offset += keySerializer.getObjectSize(inBuffer, offset);
    }

    serializedValues = new int[pageSize];
    values = (V[]) new Object[pageSize];

    if (stream == null)
      stream = new OMemoryStream(inBuffer);
    else
      stream.setSource(inBuffer);

    stream.jump(offset);

    for (int i = 0; i < size; i++) {
      serializedValues[i] = stream.getAsByteArrayOffset();
    }

    buffer = inBuffer;
  }

  /**
   * Serialize only the new values or the changed.
   * 
   */
  protected byte[] serializeStreamValue(final int iIndex) throws IOException {
    if (serializedValues[iIndex] <= 0) {
      // NEW OR MODIFIED: MARSHALL CONTENT
      PROFILER.updateCounter(PROFILER.getProcessMetric("mvrbtree.entry.serializeValue"), "Serialize a MVRBTree entry value", 1);
      return ((OMVRBTreeMapProvider<K, V>) treeDataProvider).valueSerializer.toStream(values[iIndex]);
    }
    // RETURN ORIGINAL CONTENT

    return stream.getAsByteArray(serializedValues[iIndex]);
  }

  protected Object keyFromStream(final int iIndex) throws IOException {
    return ((OMVRBTreeMapProvider<K, V>) treeDataProvider).keySerializer.deserialize(buffer, serializedKeys[iIndex]);
  }

  protected Object valueFromStream(final int iIndex) throws IOException {
    final OStreamSerializer valueSerializer = ((OMVRBTreeMapProvider<K, V>) treeDataProvider).valueSerializer;
    if (valueSerializer instanceof OBinarySerializer)
      return ((OBinarySerializer<V>) valueSerializer).deserialize(buffer, serializedValues[iIndex]);

    return valueSerializer.fromStream(stream.getAsByteArray(serializedValues[iIndex]));
  }

  private byte[] convertIntoNewSerializationFormat(byte[] stream) throws IOException {
    final OMemoryStream oldStream = new OMemoryStream(stream);

    try {
      int oldPageSize = oldStream.getAsInteger();

      ORecordId oldParentRid = new ORecordId().fromStream(oldStream.getAsByteArrayFixed(ORecordId.PERSISTENT_SIZE));
      ORecordId oldLeftRid = new ORecordId().fromStream(oldStream.getAsByteArrayFixed(ORecordId.PERSISTENT_SIZE));
      ORecordId oldRightRid = new ORecordId().fromStream(oldStream.getAsByteArrayFixed(ORecordId.PERSISTENT_SIZE));

      boolean oldColor = oldStream.getAsBoolean();
      int oldSize = oldStream.getAsInteger();

      if (oldSize > oldPageSize)
        throw new OConfigurationException("Loaded index with page size set to " + oldPageSize
            + " while the loaded was built with: " + oldSize);

      K[] oldKeys = (K[]) new Object[oldPageSize];
      for (int i = 0; i < oldSize; ++i) {
        oldKeys[i] = (K) ((OMVRBTreeMapProvider<K, V>) treeDataProvider).streamKeySerializer.fromStream(oldStream.getAsByteArray());
      }

      V[] oldValues = (V[]) new Object[oldPageSize];
      for (int i = 0; i < oldSize; ++i) {
        oldValues[i] = (V) ((OMVRBTreeMapProvider<K, V>) treeDataProvider).valueSerializer.fromStream(oldStream.getAsByteArray());
      }

      byte[] result;
      if (((OMVRBTreeMapProvider<K, V>) treeDataProvider).valueSerializer instanceof OBinarySerializer)
        result = convertNewSerializationFormatBinarySerializer(oldSize, oldPageSize, oldParentRid, oldLeftRid, oldRightRid,
            oldColor, oldKeys, oldValues);
      else
        result = convertNewSerializationFormatStreamSerializer(oldSize, oldPageSize, oldParentRid, oldLeftRid, oldRightRid,
            oldColor, oldKeys, oldValues);

      return result;

    } finally {
      oldStream.close();
    }
  }

  private byte[] convertNewSerializationFormatBinarySerializer(int oldSize, int oldPageSize, ORecordId oldParentRid,
      ORecordId oldLeftRid, ORecordId oldRightRid, boolean oldColor, K[] oldKeys, V[] oldValues) {
    int bufferSize = 2 * OIntegerSerializer.INT_SIZE;

    bufferSize += OLinkSerializer.INSTANCE.getObjectSize(oldParentRid) * 3;
    bufferSize += OBooleanSerializer.BOOLEAN_SIZE;
    bufferSize += OIntegerSerializer.INT_SIZE;

    final OBinarySerializer<K> keySerializer = ((OMVRBTreeMapProvider<K, V>) treeDataProvider).keySerializer;
    final OBinarySerializer<V> valueSerializer = (OBinarySerializer<V>) ((OMVRBTreeMapProvider<K, V>) treeDataProvider).valueSerializer;

    for (int i = 0; i < oldSize; ++i)
      bufferSize += keySerializer.getObjectSize(oldKeys[i]);

    for (int i = 0; i < oldSize; ++i)
      bufferSize += valueSerializer.getObjectSize(oldValues[i]);

    byte[] outBuffer = new byte[bufferSize];

    int offset = serializeMetadata(outBuffer, oldSize, oldPageSize, oldParentRid, oldLeftRid, oldRightRid, oldColor);

    for (int i = 0; i < oldSize; i++) {
      keySerializer.serialize(oldKeys[i], outBuffer, offset);
      offset += keySerializer.getObjectSize(oldKeys[i]);
    }

    for (int i = 0; i < oldSize; i++) {
      valueSerializer.serialize(oldValues[i], outBuffer, offset);
      offset += valueSerializer.getObjectSize(oldValues[i]);
    }

    return outBuffer;
  }

  private byte[] convertNewSerializationFormatStreamSerializer(int oldSize, int oldPageSize, ORecordId oldParentRid,
      ORecordId oldLeftRid, ORecordId oldRightRid, boolean oldColor, K[] oldKeys, V[] oldValues) throws IOException {
    int bufferSize = 2 * OIntegerSerializer.INT_SIZE;

    bufferSize += OLinkSerializer.INSTANCE.getObjectSize(oldParentRid) * 3;
    bufferSize += OBooleanSerializer.BOOLEAN_SIZE;
    bufferSize += OIntegerSerializer.INT_SIZE;

    final OBinarySerializer<K> keySerializer = ((OMVRBTreeMapProvider<K, V>) treeDataProvider).keySerializer;
    final OStreamSerializer valueSerializer = ((OMVRBTreeMapProvider<K, V>) treeDataProvider).valueSerializer;

    for (int i = 0; i < oldSize; ++i)
      bufferSize += keySerializer.getObjectSize(oldKeys[i]);

    final byte[] outBuffer = new byte[bufferSize * 2];

    int offset = serializeMetadata(outBuffer, oldSize, oldPageSize, oldParentRid, oldLeftRid, oldRightRid, oldColor);

    for (int i = 0; i < oldSize; i++) {
      keySerializer.serialize(oldKeys[i], outBuffer, offset);
      offset += keySerializer.getObjectSize(oldKeys[i]);
    }

    final OMemoryStream outStream = new OMemoryStream(outBuffer);
    try {
      outStream.jump(offset);

      for (int i = 0; i < oldSize; ++i)
        outStream.set(valueSerializer.toStream(oldValues[i]));

      return outStream.toByteArray();

    } finally {
      outStream.close();
    }
  }
}
