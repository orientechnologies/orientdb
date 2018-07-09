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

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.BytesContainer;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.OVarIntSerializer;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author marko
 */
public class ODocumentDelta {
  
  public Map<String, Object> fields = new HashMap<>();
  
  public <T> T field(String name){
    return (T)fields.get(name);
  }
  
  public void field(String name, Object value){
    fields.put(name, value);
  }
  
  public ORID getIdentity(){
    return field("i");
  }
  
  public void setIdentity(ORID identity){
    field("i", identity);
  }
  
  private void serializeString(BytesContainer bytes, String value){
    byte[] stringBytes = HelperClasses.bytesFromString(value);
    OVarIntSerializer.write(bytes, stringBytes.length);
    int start = bytes.offset;
    bytes.alloc(stringBytes.length);
    System.arraycopy(stringBytes, 0, bytes.bytes, start, stringBytes.length);
  }
  
  private String deserializeString(BytesContainer bytes){
    int stringLength = OVarIntSerializer.readAsInteger(bytes);
    byte[] stringBytes = new byte[stringLength];
    System.arraycopy(bytes.bytes, stringLength, stringBytes, 0, stringLength);
    String retValue = HelperClasses.stringFromBytes(stringBytes, 0, stringLength);
    bytes.offset += stringLength;
    return retValue;
  }
  
  private void serializeValue(Object value, BytesContainer bytes){
    
  }
  
  public byte[] serialize(){
    BytesContainer bytes = new BytesContainer();
    for (Map.Entry<String, Object> entry : fields.entrySet()){
      String fieldName = entry.getKey();
      Object fieldValue = entry.getValue();      
      serializeString(bytes, fieldName);
      OType type = OType.getTypeByClass(fieldValue.getClass());
      OVarIntSerializer.write(bytes, type.getId());
      serializeValue(fieldValue, bytes);
    }
    
    return bytes.fitBytes();
  }
  
  public void deserialize(BytesContainer bytes){
    
  }
  
}
