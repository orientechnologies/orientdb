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
package com.orientechnologies.orient.core.type.tree.provider;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.profiler.OProfilerMBean;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ORecordBytesLazy;
import com.orientechnologies.orient.core.serialization.OMemoryStream;
import com.orientechnologies.orient.core.serialization.OSerializableStream;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializerFactory;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.index.OSimpleKeySerializer;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializer;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerFactory;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerLiteral;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerLong;
import com.orientechnologies.orient.core.storage.OStorage;

public class OMVRBTreeMapProvider<K, V> extends OMVRBTreeProviderAbstract<K, V> {
  private static final long      serialVersionUID         = 1L;

  public final static byte       CURRENT_PROTOCOL_VERSION = 3;

  protected final OMemoryStream  stream;
  protected OBinarySerializer<K> keySerializer;
  protected OStreamSerializer    streamKeySerializer;

  protected OStreamSerializer    valueSerializer;
  protected boolean              keepKeysInMemory;
  protected boolean              keepValuesInMemory;

  public OMVRBTreeMapProvider(final OStorage iStorage, final String iClusterName, final ORID iRID) {
    this(iStorage, iClusterName, null, null);
    record.setIdentity(iRID.getClusterId(), iRID.getClusterPosition());
  }

  public OMVRBTreeMapProvider(final OStorage iStorage, final String iClusterName, final OBinarySerializer<K> iKeySerializer,
      final OStreamSerializer iValueSerializer) {
    super(new ORecordBytesLazy().unpin(), iStorage, iClusterName);
    ((ORecordBytesLazy) record).recycle(this);
    stream = new OMemoryStream();
    keySerializer = iKeySerializer;
    valueSerializer = iValueSerializer;
  }

  public OMVRBTreeEntryDataProvider<K, V> getEntry(final ORID iRid) {
    return new OMVRBTreeMapEntryProvider<K, V>(this, iRid);
  }

  public OMVRBTreeEntryDataProvider<K, V> createEntry() {
    return new OMVRBTreeMapEntryProvider<K, V>(this);
  }

  @Override
  public OMVRBTreeProvider<K, V> copy() {
    return new OMVRBTreeMapProvider<K, V>(storage, clusterName, keySerializer, valueSerializer);
  }

  @Override
  protected void load(final ODatabaseRecord iDb) {
    ((ORecordBytesLazy) record).recycle(this);
    super.load(iDb);
  }

  @Override
  protected void load(final OStorage iSt) {
    ((ORecordBytesLazy) record).recycle(this);
    super.load(iSt);
  }

  public boolean updateConfig() {
    final boolean changed = super.updateConfig();
    keepKeysInMemory = OGlobalConfiguration.MVRBTREE_ENTRY_KEYS_IN_MEMORY.getValueAsBoolean();
    keepValuesInMemory = OGlobalConfiguration.MVRBTREE_ENTRY_VALUES_IN_MEMORY.getValueAsBoolean();
    return changed;
  }

  public byte[] toStream() throws OSerializationException {
    final OProfilerMBean profiler = Orient.instance().getProfiler();
    final long timer = profiler.startChrono();

    try {
      stream.jump(0);
      stream.set(CURRENT_PROTOCOL_VERSION);
      stream.setAsFixed(root != null ? root.toStream() : ORecordId.EMPTY_RECORD_ID_STREAM);

      stream.set(size);
      stream.set(pageSize);
      stream.set(keySize);

      stream.set(keySerializer.getId());
      stream.set(valueSerializer.getName());

      if (streamKeySerializer != null)
        stream.set(streamKeySerializer.getName());
      else
        stream.set("");

      final byte[] result = stream.toByteArray();
      record.fromStream(result);
      return result;

    } finally {
      profiler.stopChrono(profiler.getProcessMetric("mvrbtree.toStream"), "Serialize a MVRBTree", timer);
    }
  }

  @SuppressWarnings("unchecked")
  public OSerializableStream fromStream(final byte[] iStream) throws OSerializationException {
    final OProfilerMBean profiler = Orient.instance().getProfiler();
    final long timer = profiler.startChrono();

    try {
      stream.setSource(iStream);
      byte protocolVersion = stream.peek();
      if (protocolVersion != -1) {
        // @COMPATIBILITY BEFORE 0.9.25
        stream.getAsByte();
        if (protocolVersion != CURRENT_PROTOCOL_VERSION)
          OLogManager
              .instance()
              .debug(
                  this,
                  "Found tree %s created with MVRBTree protocol version %d while current one supports the version %d. The tree will be migrated transparently",
                  getRecord().getIdentity(), protocolVersion, CURRENT_PROTOCOL_VERSION);
      }

      root = new ORecordId();
      root.fromStream(stream.getAsByteArrayFixed(ORecordId.PERSISTENT_SIZE));

      size = stream.getAsInteger();
      if (protocolVersion == -1)
        // @COMPATIBILITY BEFORE 0.9.25
        pageSize = stream.getAsShort();
      else
        pageSize = stream.getAsInteger();

      // @COMPATIBILITY BEFORE 1.0
      if (protocolVersion < 1) {
        keySize = 1;
        OLogManager.instance().warn(this,
            "Previous index version was found, partial composite index queries may do not work if you " + "do not recreate index.");
      } else
        keySize = stream.getAsInteger();

      // @COMPATIBILITY BEFORE 1.0
      if (protocolVersion < 3) {
        streamKeySerializer = OStreamSerializerFactory.get(stream.getAsString());
        valueSerializer = OStreamSerializerFactory.get(stream.getAsString());

        keySerializer = createRelatedSerializer(streamKeySerializer);
      } else {
        keySerializer = (OBinarySerializer<K>) OBinarySerializerFactory.INSTANCE.getObjectSerializer(stream.getAsByte());
        valueSerializer = OStreamSerializerFactory.get(stream.getAsString());

        final String oldKeySerializerName = stream.getAsString();
        if (oldKeySerializerName != null && oldKeySerializerName.length() > 0)
          streamKeySerializer = OStreamSerializerFactory.get(oldKeySerializerName);
      }
    } catch (Exception e) {
      OLogManager.instance().error(this, "Error on unmarshalling OMVRBTreeMapProvider object from record: %s", e,
          OSerializationException.class, root);
    } finally {
      profiler.stopChrono(profiler.getProcessMetric("mvrbtree.fromStream"), "Deserialize a MVRBTree", timer);
    }
    return this;
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  public OBinarySerializer<K> createRelatedSerializer(final OStreamSerializer streamKeySerializer) {
    if (streamKeySerializer instanceof OBinarySerializer)
      return (OBinarySerializer<K>) streamKeySerializer;

    if (streamKeySerializer instanceof OStreamSerializerLiteral)
      return (OBinarySerializer<K>) new OSimpleKeySerializer();

    if (streamKeySerializer instanceof OStreamSerializerLong)
      return (OBinarySerializer<K>) OLongSerializer.INSTANCE;

    throw new OSerializationException("Given serializer " + streamKeySerializer.getClass().getName()
        + " can not be converted into " + OBinarySerializer.class.getName() + ".");
  }

  public OBinarySerializer<K> getKeySerializer() {
    return keySerializer;
  }

  public OStreamSerializer getValueSerializer() {
    return valueSerializer;
  }
}
