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
package com.orientechnologies.orient.core.serialization.serializer.string;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.OBinaryProtocol;
import com.orientechnologies.orient.core.serialization.ODocumentSerializable;
import com.orientechnologies.orient.core.serialization.OSerializableStream;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerHelper;

public class OStringSerializerEmbedded implements OStringSerializer {
  public static final OStringSerializerEmbedded INSTANCE = new OStringSerializerEmbedded();
  public static final String                    NAME     = "em";

  /**
   * Re-Create any object if the class has a public constructor that accepts a String as unique parameter.
   */
  public Object fromStream(final String iStream) {
    if (iStream == null || iStream.length() == 0)
      // NULL VALUE
      return null;

    final ODocument instance = new ODocument();
    instance.fromStream(OBinaryProtocol.string2bytes(iStream));

    final String className = instance.field(ODocumentSerializable.CLASS_NAME);
    if (className == null)
      return instance;

    Class<?> clazz = null;
    try {
      clazz = Class.forName(className);
    } catch (ClassNotFoundException e) {
      OLogManager.instance().debug(this, "Class name provided in embedded document " + className + " does not exist.");
    }

    if (clazz == null)
      return instance;

    if (ODocumentSerializable.class.isAssignableFrom(clazz)) {
      try {
        final ODocumentSerializable documentSerializable = (ODocumentSerializable) clazz.newInstance();
        final ODocument docClone = new ODocument();
        instance.copyTo(docClone);
        docClone.removeField(ODocumentSerializable.CLASS_NAME);
        documentSerializable.fromDocument(docClone);

        return documentSerializable;
      } catch (InstantiationException e) {
        throw OException.wrapException(new OSerializationException("Cannot serialize the object"), e);
      } catch (IllegalAccessException e) {
        throw OException.wrapException(new OSerializationException("Cannot serialize the object"), e);
      }
    }

    return instance;
  }

  /**
   * Serialize the class name size + class name + object content
   * 
   * @param iValue
   */
  public StringBuilder toStream(final StringBuilder iOutput, Object iValue) {
    if (iValue != null) {
      if (iValue instanceof ODocumentSerializable)
        iValue = ((ODocumentSerializable) iValue).toDocument();

      if (!(iValue instanceof OSerializableStream))
        throw new OSerializationException("Cannot serialize the object since it's not implements the OSerializableStream interface");

      OSerializableStream stream = (OSerializableStream) iValue;
      iOutput.append(iValue.getClass().getName());
      iOutput.append(OStreamSerializerHelper.SEPARATOR);
      iOutput.append(OBinaryProtocol.bytes2string(stream.toStream()));
    }

    return iOutput;
  }

  public String getName() {
    return NAME;
  }
}
