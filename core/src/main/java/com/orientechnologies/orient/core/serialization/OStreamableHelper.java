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
package com.orientechnologies.orient.core.serialization;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.exception.OSerializationException;

import java.io.*;
import java.lang.ref.Reference;
import java.lang.ref.WeakReference;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Helper class to serialize OStreamable objects.
 * 
 * @author Luca Garulli (l.garulli--at--orientdb.com)
 * 
 */
public class OStreamableHelper {
  final static byte NULL         = 0;
  final static byte STREAMABLE   = 1;
  final static byte SERIALIZABLE = 2;

  final static byte STRING       = 10;
  final static byte INTEGER      = 11;
  final static byte SHORT        = 12;
  final static byte LONG         = 13;
  final static byte BOOLEAN      = 14;

  // uses weak references to avoid holding onto types when their defining module is unloaded
  // (we only expect a few types, so keep it simple and don't bother to evict dangling keys)
  private static final Map<String, Reference<Class<?>>> streamableTypes = new ConcurrentHashMap<>();

  /**
   * Use this to register {@link OStreamable} types belonging to non-core OrientDB modules.
   */
  public static void registerType(final Class<? extends OStreamable> type) {
    streamableTypes.put(type.getName(), new WeakReference<>(type));
  }

  public static void toStream(final DataOutput out, final Object object) throws IOException {
    if (object == null)
      out.writeByte(NULL);
    else if (object instanceof OStreamable) {
      out.writeByte(STREAMABLE);
      out.writeUTF(object.getClass().getName());
      ((OStreamable) object).toStream(out);
    } else if (object instanceof String) {
      out.writeByte(STRING);
      out.writeUTF((String) object);
    } else if (object instanceof Integer) {
      out.writeByte(INTEGER);
      out.writeInt((Integer) object);
    } else if (object instanceof Short) {
      out.writeByte(SHORT);
      out.writeShort((Short) object);
    } else if (object instanceof Long) {
      out.writeByte(LONG);
      out.writeLong((Long) object);
    } else if (object instanceof Boolean) {
      out.writeByte(BOOLEAN);
      out.writeBoolean((Boolean) object);
    } else if (object instanceof Serializable) {
      out.writeByte(SERIALIZABLE);
      final ByteArrayOutputStream mem = new ByteArrayOutputStream();
      final ObjectOutputStream oos = new ObjectOutputStream(mem);
      try {
        oos.writeObject(object);
        oos.flush();
        final byte[] buffer = mem.toByteArray();
        out.writeInt(buffer.length);
        out.write(buffer);
      } finally {
        oos.close();
        mem.close();
      }
    } else
      throw new OSerializationException("Object not supported: " + object);

  }

  public static Object fromStream(final DataInput in) throws IOException {
    Object object = null;

    final byte objectType = in.readByte();
    switch (objectType) {
    case NULL:
      return null;
    case STREAMABLE:
      final String payloadClassName = in.readUTF();
      try {
        if (streamableTypes.containsKey(payloadClassName)) {
          object = streamableTypes.get(payloadClassName).get().newInstance();
        } else {
          object = Class.forName(payloadClassName).newInstance();
        }
        ((OStreamable) object).fromStream(in);
      } catch (Exception e) {
        OException.wrapException(new OSerializationException("Cannot unmarshall object from distributed request"), e);
      }
      break;
    case SERIALIZABLE:
      final byte[] buffer = new byte[in.readInt()];
      in.readFully(buffer);
      final ByteArrayInputStream mem = new ByteArrayInputStream(buffer);
      final ObjectInputStream ois = new ObjectInputStream(mem);
      try {
        try {
          object = ois.readObject();
        } catch (ClassNotFoundException e) {
          OException.wrapException(new OSerializationException("Cannot unmarshall object from distributed request"), e);
        }
      } finally {
        ois.close();
        mem.close();
      }
      break;
    case STRING:
      return in.readUTF();
    case INTEGER:
      return in.readInt();
    case SHORT:
      return in.readShort();
    case LONG:
      return in.readLong();
    case BOOLEAN:
      return in.readBoolean();
    default:
      throw new OSerializationException("Object type not supported: " + objectType);
    }
    return object;
  }

}
