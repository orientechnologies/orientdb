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

package com.orientechnologies.orient.core.serialization.serializer.binary;

import java.util.HashMap;
import java.util.Map;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OBinaryTypeSerializer;
import com.orientechnologies.common.serialization.types.OBooleanSerializer;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OCharSerializer;
import com.orientechnologies.common.serialization.types.ODateSerializer;
import com.orientechnologies.common.serialization.types.ODateTimeSerializer;
import com.orientechnologies.common.serialization.types.ODecimalSerializer;
import com.orientechnologies.common.serialization.types.ODoubleSerializer;
import com.orientechnologies.common.serialization.types.OFloatSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.serialization.types.ONullSerializer;
import com.orientechnologies.common.serialization.types.OShortSerializer;
import com.orientechnologies.common.serialization.types.OStringSerializer;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OLinkSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.index.OCompositeKeySerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.index.OSimpleKeySerializer;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerRID;

/**
 * This class is responsible for obtaining OBinarySerializer realization, by it's id of type of object that should be serialized.
 * 
 * 
 * @author <a href="mailto:gmandnepr@gmail.com">Evgeniy Degtiarenko</a>
 */
public class OBinarySerializerFactory {

  private final Map<Byte, OBinarySerializer<?>>                  serializerIdMap        = new HashMap<Byte, OBinarySerializer<?>>();
  private final Map<Byte, Class<? extends OBinarySerializer<?>>> serializerClassesIdMap = new HashMap<Byte, Class<? extends OBinarySerializer<?>>>();
  private final Map<OType, OBinarySerializer<?>>                 serializerTypeMap      = new HashMap<OType, OBinarySerializer<?>>();

  /**
   * Instance of the factory
   */
  public static final OBinarySerializerFactory                   INSTANCE               = new OBinarySerializerFactory();
  /**
   * Size of the type identifier block size
   */
  public static final int                                        TYPE_IDENTIFIER_SIZE   = 1;

  private OBinarySerializerFactory() {

    // STATELESS SERIALIER
    registerSerializer(new ONullSerializer(), null);

    registerSerializer(OBooleanSerializer.INSTANCE, OType.BOOLEAN);
    registerSerializer(OIntegerSerializer.INSTANCE, OType.INTEGER);
    registerSerializer(OShortSerializer.INSTANCE, OType.SHORT);
    registerSerializer(OLongSerializer.INSTANCE, OType.LONG);
    registerSerializer(OFloatSerializer.INSTANCE, OType.FLOAT);
    registerSerializer(ODoubleSerializer.INSTANCE, OType.DOUBLE);
    registerSerializer(ODateTimeSerializer.INSTANCE, OType.DATETIME);
    registerSerializer(OCharSerializer.INSTANCE, null);
    registerSerializer(OStringSerializer.INSTANCE, OType.STRING);
    registerSerializer(OByteSerializer.INSTANCE, OType.BYTE);
    registerSerializer(ODateSerializer.INSTANCE, OType.DATE);
    registerSerializer(OLinkSerializer.INSTANCE, OType.LINK);
    registerSerializer(OCompositeKeySerializer.INSTANCE, null);
    registerSerializer(OStreamSerializerRID.INSTANCE, null);
    registerSerializer(OBinaryTypeSerializer.INSTANCE, OType.BINARY);
    registerSerializer(ODecimalSerializer.INSTANCE, OType.DECIMAL);

    // STATEFUL SERIALIER
    registerSerializer(OSimpleKeySerializer.ID, OSimpleKeySerializer.class);
  }

  public void registerSerializer(final OBinarySerializer<?> iInstance, final OType iType) {
    serializerIdMap.put(iInstance.getId(), iInstance);
    if (iType != null)
      serializerTypeMap.put(iType, iInstance);
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  public void registerSerializer(final byte iId, final Class<? extends OBinarySerializer> iClass) {
    serializerClassesIdMap.put(iId, (Class<? extends OBinarySerializer<?>>) iClass);
  }

  /**
   * Obtain OBinarySerializer instance by it's id.
   * 
   * @param identifier
   *          is serializes identifier.
   * @return OBinarySerializer instance.
   */
  public OBinarySerializer<?> getObjectSerializer(final byte identifier) {
    OBinarySerializer<?> impl = serializerIdMap.get(identifier);
    if (impl == null) {
      final Class<? extends OBinarySerializer<?>> cls = serializerClassesIdMap.get(identifier);
      if (cls != null)
        try {
          impl = cls.newInstance();
        } catch (Exception e) {
          OLogManager.instance().error(this, "Cannot create an instance of class %s invoking the empty constructor", cls);
        }
    }
    return impl;
  }

  /**
   * Obtain OBinarySerializer realization for the OType
   * 
   * @param type
   *          is the OType to obtain serializer algorithm for
   * @return OBinarySerializer instance
   */
  public OBinarySerializer<?> getObjectSerializer(final OType type) {
    return serializerTypeMap.get(type);
  }
}
