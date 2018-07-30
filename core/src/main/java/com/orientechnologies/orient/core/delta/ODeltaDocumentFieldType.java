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
import com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author marko
 */
public enum ODeltaDocumentFieldType {
  BOOLEAN(0),
  INTEGER(1),
  SHORT(2),
  LONG(3),
  FLOAT(4),
  DOUBLE(5),
  DATETIME(6),
  STRING(7),
  BINARY(8),
  EMBEDDED(9),
  EMBEDDEDLIST(10),
  EMBEDDEDSET(11),
  EMBEDDEDMAP(12),
  LINK(13),
  LINKLIST(14),
  LINKSET(15),
  LINKMAP(16),
  BYTE(17),
  TRANSIENT(18),
  DATE(19),
  CUSTOM(20),
  DECIMAL(21),
  LINKBAG(22),
  ANY(23),
  DELTA_RECORD(24);
  
  private final int id;
  private static final Map<Integer, ODeltaDocumentFieldType> mappedIds = new HashMap<>();
  
  static{
    for (ODeltaDocumentFieldType type : values()){
      mappedIds.put(type.id, type);
    }
  }
  
  ODeltaDocumentFieldType(int id){
    this.id = id;
  }
  
  public int getId(){
    return id;
  }
  
  protected static ODeltaDocumentFieldType getFromClass(Class claz){
    if (claz == null){
      return null;
    }
    
    if (claz.equals(ODocumentDelta.class)){
      return DELTA_RECORD;
    }
    
    OType type = OType.getTypeByClass(claz);
    if (mappedIds.containsKey(type.getId())){
      return mappedIds.get(type.getId());
    }
    else{
      return null;
    }
  }
  
  public static ODeltaDocumentFieldType getFromId(int id){
    return mappedIds.get(id);
  }
  
  public static ODeltaDocumentFieldType getTypeByValue(Object value){
    if (value == null){
      return null;
    }
    
    if (value instanceof ODocumentDelta){
      return DELTA_RECORD;
    }
    
    OType type = OType.getTypeByValue(value);
    if (mappedIds.containsKey(type.getId())){
      return mappedIds.get(type.getId());
    }
    else{
      return null;
    }
  }
}
