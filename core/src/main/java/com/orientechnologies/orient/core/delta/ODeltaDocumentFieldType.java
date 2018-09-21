/*
 * Copyright 2018 OrientDB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.delta;

import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.schema.OTypeInterface;

import java.util.HashMap;
import java.util.Map;

/**
 * @author marko
 */
public enum ODeltaDocumentFieldType implements OTypeInterface {

  DELTA_RECORD(Byte.MAX_VALUE);

  private final        int                                   id;
  private static final Map<Integer, ODeltaDocumentFieldType> mappedIds = new HashMap<>();

  static {
    for (ODeltaDocumentFieldType type : values()) {
      mappedIds.put(type.id, type);
    }
  }

  ODeltaDocumentFieldType(int id) {
    this.id = id;
  }

  @Override
  public int getId() {
    return id;
  }

  protected static OTypeInterface getFromClass(Class claz) {
    if (claz == null) {
      return null;
    }

    if (claz.equals(ODocumentDelta.class)) {
      return DELTA_RECORD;
    }

    OType type = OType.getTypeByClass(claz);
    return type;
  }

  public static OTypeInterface getFromId(int id) {
    if (mappedIds.containsKey(id)) {
      return mappedIds.get(id);
    }

    OType baseType = OType.getById((byte) id);
    return baseType;
  }

  public static OTypeInterface getTypeByValue(Object value) {
    if (value == null) {
      return null;
    }

    if (value instanceof ODocumentDelta) {
      return DELTA_RECORD;
    }

    OType type = OType.getTypeByValue(value);
    return type;
  }

  @Override
  public boolean isList() {
    return false;
  }
}
