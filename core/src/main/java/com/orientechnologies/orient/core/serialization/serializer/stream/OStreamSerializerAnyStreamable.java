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
package com.orientechnologies.orient.core.serialization.serializer.stream;

import java.io.IOException;
import java.util.Arrays;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OArrays;
import com.orientechnologies.orient.core.command.script.OCommandScript;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.query.OQuery;
import com.orientechnologies.orient.core.serialization.OBinaryProtocol;
import com.orientechnologies.orient.core.serialization.OSerializableStream;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

public class OStreamSerializerAnyStreamable implements OStreamSerializer {
  private static final String                        SCRIPT_COMMAND_CLASS         = "s";
  private static final byte[]                        SCRIPT_COMMAND_CLASS_ASBYTES = SCRIPT_COMMAND_CLASS.getBytes();
  private static final String                        SQL_COMMAND_CLASS            = "c";
  private static final byte[]                        SQL_COMMAND_CLASS_ASBYTES    = SQL_COMMAND_CLASS.getBytes();
  private static final String                        QUERY_COMMAND_CLASS          = "q";
  private static final byte[]                        QUERY_COMMAND_CLASS_ASBYTES  = QUERY_COMMAND_CLASS.getBytes();

  public static final OStreamSerializerAnyStreamable INSTANCE                     = new OStreamSerializerAnyStreamable();
  public static final String                         NAME                         = "at";

  /**
   * Re-Create any object if the class has a public constructor that accepts a String as unique parameter.
   */
  public Object fromStream(final byte[] iStream) throws IOException {
    if (iStream == null || iStream.length == 0)
      // NULL VALUE
      return null;

    final int classNameSize = OBinaryProtocol.bytes2int(iStream);
    if (classNameSize <= 0)
      OLogManager.instance().error(this, "Class signature not found in ANY element: " + Arrays.toString(iStream),
          OSerializationException.class);

    final String className = OBinaryProtocol.bytes2string(iStream, 4, classNameSize);

    try {
      final OSerializableStream stream;
      // CHECK FOR ALIASES
      if (className.equalsIgnoreCase("q"))
        // QUERY
        stream = new OSQLSynchQuery<Object>();
      else if (className.equalsIgnoreCase("c"))
        // SQL COMMAND
        stream = new OCommandSQL();
      else if (className.equalsIgnoreCase("s"))
        // SCRIPT COMMAND
        stream = new OCommandScript();
      else
        // CREATE THE OBJECT BY INVOKING THE EMPTY CONSTRUCTOR
        stream = (OSerializableStream) Class.forName(className).newInstance();

      return stream.fromStream(OArrays.copyOfRange(iStream, 4 + classNameSize, iStream.length));

    } catch (Exception e) {
      OLogManager.instance().error(this, "Error on unmarshalling content. Class: " + className, e, OSerializationException.class);
    }
    return null;
  }

  /**
   * Serialize the class name size + class name + object content
   */
  public byte[] toStream(final Object iObject) throws IOException {
    if (iObject == null)
      return null;

    if (!(iObject instanceof OSerializableStream))
      throw new OSerializationException("Cannot serialize the object [" + iObject.getClass() + ":" + iObject
          + "] since it does not implement the OSerializableStream interface");

    OSerializableStream stream = (OSerializableStream) iObject;

    // SERIALIZE THE CLASS NAME
    final byte[] className;
    if (iObject instanceof OQuery<?>)
      className = QUERY_COMMAND_CLASS_ASBYTES;
    else if (iObject instanceof OCommandSQL)
      className = SQL_COMMAND_CLASS_ASBYTES;
    else if (iObject instanceof OCommandScript)
      className = SCRIPT_COMMAND_CLASS_ASBYTES;
    else
      className = OBinaryProtocol.string2bytes(iObject.getClass().getName());

    // SERIALIZE THE OBJECT CONTENT
    byte[] objectContent = stream.toStream();

    byte[] result = new byte[4 + className.length + objectContent.length];

    // COPY THE CLASS NAME SIZE + CLASS NAME + OBJECT CONTENT
    System.arraycopy(OBinaryProtocol.int2bytes(className.length), 0, result, 0, 4);
    System.arraycopy(className, 0, result, 4, className.length);
    System.arraycopy(objectContent, 0, result, 4 + className.length, objectContent.length);

    return result;
  }

  public String getName() {
    return NAME;
  }
}
