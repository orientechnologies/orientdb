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
package com.orientechnologies.orient.core.serialization.serializer.stream;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OArrays;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.command.script.OCommandScript;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.serialization.OBinaryProtocol;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OLiveQuery;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.io.IOException;
import java.util.Arrays;

public class OStreamSerializerAnyStreamable {
  private static final String SCRIPT_COMMAND_CLASS = "s";
  private static final byte[] SCRIPT_COMMAND_CLASS_ASBYTES = SCRIPT_COMMAND_CLASS.getBytes();
  private static final String SQL_COMMAND_CLASS = "c";
  private static final byte[] SQL_COMMAND_CLASS_ASBYTES = SQL_COMMAND_CLASS.getBytes();
  private static final String QUERY_COMMAND_CLASS = "q";
  private static final byte[] QUERY_COMMAND_CLASS_ASBYTES = QUERY_COMMAND_CLASS.getBytes();

  public static final OStreamSerializerAnyStreamable INSTANCE =
      new OStreamSerializerAnyStreamable();
  public static final String NAME = "at";

  /**
   * Re-Create any object if the class has a public constructor that accepts a String as unique
   * parameter.
   */
  public OCommandRequestText fromStream(final byte[] iStream, ORecordSerializer serializer)
      throws IOException {
    if (iStream == null || iStream.length == 0)
      // NULL VALUE
      return null;

    final int classNameSize = OBinaryProtocol.bytes2int(iStream);

    if (classNameSize <= 0) {
      final String message =
          "Class signature not found in ANY element: " + Arrays.toString(iStream);
      OLogManager.instance().error(this, message, null);

      throw new OSerializationException(message);
    }

    final String className = new String(iStream, 4, classNameSize, "UTF-8");

    try {
      final OCommandRequestText stream;
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
        stream = (OCommandRequestText) Class.forName(className).newInstance();

      return stream.fromStream(
          OArrays.copyOfRange(iStream, 4 + classNameSize, iStream.length), serializer);

    } catch (Exception e) {
      final String message = "Error on unmarshalling content. Class: " + className;
      OLogManager.instance().error(this, message, e);
      throw OException.wrapException(new OSerializationException(message), e);
    }
  }

  /** Serialize the class name size + class name + object content */
  public byte[] toStream(final OCommandRequestText iObject) throws IOException {
    if (iObject == null) return null;

    // SERIALIZE THE CLASS NAME
    final byte[] className;
    if (iObject instanceof OLiveQuery<?>)
      className = iObject.getClass().getName().getBytes("UTF-8");
    else if (iObject instanceof OSQLSynchQuery<?>) className = QUERY_COMMAND_CLASS_ASBYTES;
    else if (iObject instanceof OCommandSQL) className = SQL_COMMAND_CLASS_ASBYTES;
    else if (iObject instanceof OCommandScript) className = SCRIPT_COMMAND_CLASS_ASBYTES;
    else {
      if (iObject == null) className = null;
      else className = iObject.getClass().getName().getBytes("UTF-8");
    }
    // SERIALIZE THE OBJECT CONTENT
    byte[] objectContent = iObject.toStream();

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
