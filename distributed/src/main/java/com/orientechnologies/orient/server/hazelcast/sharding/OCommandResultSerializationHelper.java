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
package com.orientechnologies.orient.server.hazelcast.sharding;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.serialization.OBinaryProtocol;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerStringAbstract;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;

/**
 * This class provides ability to serialize and deserialize query execution result using default streams
 * 
 * @author gman
 * @since 20.09.12 15:56
 */
public class OCommandResultSerializationHelper {
  public static byte[] writeToStream(Object result) throws IOException {
    final ByteArrayOutputStream stream = new ByteArrayOutputStream();
    writeToStream(result, stream);
    return stream.toByteArray();
  }

  public static Object readFromStream(byte[] data) throws IOException {
    return readFromStream(new ByteArrayInputStream(data));
  }

  public static void writeToStream(Object result, OutputStream stream) throws IOException {
    if (result == null) {
      stream.write((byte) 'n');
    } else if (result instanceof OIdentifiable) {
      stream.write((byte) 'r');
      writeIdentifiable((OIdentifiable) result, stream);
    } else if (result instanceof Collection<?>) {
      final Collection<OIdentifiable> list = (Collection<OIdentifiable>) result;
      stream.write((byte) 'l');
      stream.write(OBinaryProtocol.int2bytes(list.size()));
      for (OIdentifiable o : list) {
        writeIdentifiable(o, stream);
      }
    } else if (result instanceof Integer) {
      stream.write((byte) 'i');
      stream.write(OBinaryProtocol.int2bytes((Integer) result));
    } else {
      stream.write((byte) 'a');
      final StringBuilder value = new StringBuilder();
      ORecordSerializerStringAbstract.fieldTypeToString(value, OType.getTypeByClass(result.getClass()), result);
      final byte[] bytes = value.toString().getBytes();
      stream.write(OBinaryProtocol.int2bytes(bytes.length));
      stream.write(bytes);
    }
  }

  public static Object readFromStream(InputStream stream) throws IOException {
    final byte type = (byte) stream.read();
    switch (type) {
    case 'n': {
      return null;
    }
    case 'r': {
      return readIdentifiable(stream);
    }
    case 'l': {
      final int size = OBinaryProtocol.bytes2int(stream);
      final List<OIdentifiable> result = new ArrayList<OIdentifiable>(size);
      for (int i = 0; i < size; i++) {
        result.add(readIdentifiable(stream));
      }
      return result;
    }
    case 'i': {
      return OBinaryProtocol.bytes2int(stream);
    }
    case 'a': {
      final int len = OBinaryProtocol.bytes2int(stream);
      final byte[] bytes = readFully(stream, 0, len);
      final String value = new String(bytes);
      return ORecordSerializerStringAbstract.fieldTypeFromStream(null, ORecordSerializerStringAbstract.getType(value), value);
    }
    }
    return null;
  }

  public static void writeIdentifiable(OIdentifiable o, OutputStream stream) throws IOException {
    if (o == null) {
      stream.write(OBinaryProtocol.short2bytes(OChannelBinaryProtocol.RECORD_NULL));
    } else if (o instanceof ORecordId) {
      stream.write(OBinaryProtocol.short2bytes(OChannelBinaryProtocol.RECORD_RID));
      writeRecordId((ORecordId) o, stream);
    } else {
      stream.write(OBinaryProtocol.short2bytes((short) 0));
      writeRecordInternal((ORecordInternal<?>) o, stream);
    }
  }

  public static OIdentifiable readIdentifiable(InputStream stream) throws IOException {
    final short classId = (short) OBinaryProtocol.bytes2short(stream);
    switch (classId) {
    case OChannelBinaryProtocol.RECORD_NULL: {
      return null;
    }
    case OChannelBinaryProtocol.RECORD_RID: {
      return readRecordId(stream);
    }
    default: {
      return readRecordInternal(stream);
    }
    }
  }

  public static void writeRecordId(ORID id, OutputStream stream) throws IOException {
    stream.write(OBinaryProtocol.short2bytes((short) id.getClusterId()));
    stream.write(OBinaryProtocol.long2bytes(id.getClusterPosition()));
  }

  public static ORecordId readRecordId(InputStream stream) throws IOException {
    final short clusterId = (short) OBinaryProtocol.bytes2short(stream);
    final long clusterPosition = OBinaryProtocol.bytes2long(stream);
    return new ORecordId(clusterId, clusterPosition);
  }

  public static void writeRecordInternal(ORecordInternal<?> record, OutputStream stream) throws IOException {
    stream.write(record.getRecordType());
    writeRecordId(record.getIdentity(), stream);
    stream.write(OBinaryProtocol.int2bytes(record.getVersion()));
    try {
      final byte[] bytes = record.toStream();
      stream.write(OBinaryProtocol.int2bytes(bytes.length));
      stream.write(bytes);
    } catch (Exception e) {
      OLogManager.instance().error(null, "Error on unmarshalling record " + record.getIdentity().toString(),
          OSerializationException.class);
    }
  }

  public static ORecordInternal<?> readRecordInternal(InputStream stream) throws IOException {
    final byte recordType = (byte) stream.read();
    final ORecordInternal<?> record = Orient.instance().getRecordFactoryManager().newInstance(recordType);
    final ORecordId recordId = readRecordId(stream);
    final int version = OBinaryProtocol.bytes2int(stream);
    final int length = OBinaryProtocol.bytes2int(stream);
    final byte[] bytes = readFully(stream, 0, length);
    record.fill(recordId, version, bytes, false);

    return record;
  }

  public static byte[] readFully(InputStream stream, int off, int len) throws IOException {
    if (len < 0)
      throw new IndexOutOfBoundsException();
    final byte[] bytes = new byte[len];
    int n = 0;
    while (n < len) {
      int count = stream.read(bytes, off + n, len - n);
      if (count < 0) {
        throw new EOFException();
      }
      n += count;
    }
    return bytes;
  }
}
