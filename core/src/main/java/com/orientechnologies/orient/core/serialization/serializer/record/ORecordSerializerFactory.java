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

import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerBinary;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerJSON;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerSchemaAware2CSV;

/**
 * Factory of record serialized.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class ORecordSerializerFactory {
  private static final ORecordSerializerFactory instance        = new ORecordSerializerFactory();

  private Map<String, ORecordSerializer>        implementations = new HashMap<String, ORecordSerializer>();

  @Deprecated
  private ORecordSerializer                     defaultRecordFormat;

  public ORecordSerializerFactory() {
    defaultRecordFormat = new ORecordSerializerRaw();

    register(ORecordSerializerSchemaAware2CSV.NAME, ORecordSerializerSchemaAware2CSV.INSTANCE);
    register(ORecordSerializerJSON.NAME, ORecordSerializerJSON.INSTANCE);
    register(ORecordSerializerRaw.NAME, defaultRecordFormat);
    register(ORecordSerializerBinary.NAME, ORecordSerializerBinary.INSTANCE);
  }

  /**
   * Registers record serializer implementation.
   * 
   * @param iName
   *          Name to register, use JSON to overwrite default JSON serializer
   * @param iInstance
   *          Serializer implementation
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

  // Never used so can be deprecate.
  @Deprecated
  public ORecordSerializer getFormatForObject(final Object iObject, final String iFormatName) {
    if (iObject == null)
      return null;

    ORecordSerializer recordFormat = null;
    if (iFormatName != null)
      recordFormat = implementations.get(iObject.getClass().getSimpleName() + "2" + iFormatName);

    if (recordFormat == null)
      recordFormat = defaultRecordFormat;

    return recordFormat;
  }

  @Deprecated
  public ORecordSerializer getDefaultRecordFormat() {
    return defaultRecordFormat;
  }

  @Deprecated
  public void setDefaultRecordFormat(final ORecordSerializer iDefaultFormat) {
    this.defaultRecordFormat = iDefaultFormat;
  }

  public static ORecordSerializerFactory instance() {
    return instance;
  }
}
