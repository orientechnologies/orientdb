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
package com.orientechnologies.orient.core.serialization;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.exception.OSerializationException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.io.Serializable;

/**
 * Helper class to serialize OStreamable objects.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OStreamableHelper {
  static final byte NULL = 0;
  static final byte STREAMABLE = 1;
  static final byte SERIALIZABLE = 2;

  static final byte STRING = 10;
  static final byte INTEGER = 11;
  static final byte SHORT = 12;
  static final byte LONG = 13;
  static final byte BOOLEAN = 14;

  private static ClassLoader streamableClassLoader;

  /** Set the preferred {@link ClassLoader} used to load streamable types. */
  public static void setStreamableClassLoader(
      /* @Nullable */ final ClassLoader streamableClassLoader) {
    OStreamableHelper.streamableClassLoader = streamableClassLoader;
  }

  public static void toStream(final DataOutput out, final Object object) throws IOException {
    if (object == null) out.writeByte(NULL);
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
    } else throw new OSerializationException("Object not supported: " + object);
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
          if (streamableClassLoader != null) {
            object = streamableClassLoader.loadClass(payloadClassName).newInstance();
          } else {
            object = Class.forName(payloadClassName).newInstance();
          }
          ((OStreamable) object).fromStream(in);
        } catch (Exception e) {
          OException.wrapException(
              new OSerializationException("Cannot unmarshall object from distributed request"), e);
        }
        break;
      case SERIALIZABLE:
        final byte[] buffer = new byte[in.readInt()];
        in.readFully(buffer);
        final ByteArrayInputStream mem = new ByteArrayInputStream(buffer);
        final ObjectInputStream ois;
        if (streamableClassLoader != null) {
          ois =
              new ObjectInputStream(mem) {
                @Override
                protected Class<?> resolveClass(ObjectStreamClass desc)
                    throws IOException, ClassNotFoundException {
                  return streamableClassLoader.loadClass(desc.getName());
                }
              };
        } else {
          ois = new ObjectInputStream(mem);
        }
        try {
          try {
            object = ois.readObject();
          } catch (ClassNotFoundException e) {
            OException.wrapException(
                new OSerializationException("Cannot unmarshall object from distributed request"),
                e);
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
