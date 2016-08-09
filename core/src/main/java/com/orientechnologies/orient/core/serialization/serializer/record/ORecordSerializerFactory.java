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
package com.orientechnologies.orient.core.serialization.serializer.record;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerBinary;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetwork;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerJSON;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerSchemaAware2CSV;

/**
 * Factory of record serialized.
 *
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class ORecordSerializerFactory {
  private static final ORecordSerializerFactory       instance        = new ORecordSerializerFactory();
  private              Map<String, ORecordSerializer> implementations = new HashMap<String, ORecordSerializer>();

  private ORecordSerializer defaultRecordSerializer;

  public ORecordSerializerFactory() {
    register(ORecordSerializerSchemaAware2CSV.NAME, ORecordSerializerSchemaAware2CSV.INSTANCE);
    register(ORecordSerializerJSON.NAME, ORecordSerializerJSON.INSTANCE);
    register(ORecordSerializerRaw.NAME, new ORecordSerializerRaw());
    register(ORecordSerializerBinary.NAME, ORecordSerializerBinary.INSTANCE);
    register(ORecordSerializerNetwork.NAME, ORecordSerializerNetwork.INSTANCE);
    defaultRecordSerializer = getFormat(OGlobalConfiguration.DB_DOCUMENT_SERIALIZER.getValueAsString());
    if (defaultRecordSerializer == null)
      throw new ODatabaseException(
          "Impossible to find serializer with name " + OGlobalConfiguration.DB_DOCUMENT_SERIALIZER.getValueAsString());
  }

  /**
   * Registers record serializer implementation.
   *
   * @param iName     Name to register, use JSON to overwrite default JSON serializer
   * @param iInstance Serializer implementation
   */
  public void register(final String iName, final ORecordSerializer iInstance) {
    implementations.put(iName, iInstance);
  }

  public Collection<ORecordSerializer> getFormats() {
    return implementations.values();
  }

  public ORecordSerializer getFormat(final String iFormatName) {
    if (iFormatName == null)
      return null;

    return implementations.get(iFormatName);
  }

  public static ORecordSerializerFactory instance() {
    return instance;
  }

  public void setDefaultRecordSerializer(ORecordSerializer defaultRecordSerializer) {
    this.defaultRecordSerializer = defaultRecordSerializer;
  }
  
  public ORecordSerializer getDefaultRecordSerializer() {
    return defaultRecordSerializer;
  }
}
