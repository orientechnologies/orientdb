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
package com.orientechnologies.orient.core.serialization.serializer.binary.impl;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.BytesContainer;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerBinary;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.OResultBinary;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.OVarIntSerializer;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 *
 * @author mdjurovi
 */
@RunWith(Parameterized.class)
public class ORecordSerializerBinaryTest {
  
  private static ODatabaseDocumentTx db = null;
  private final ORecordSerializerBinary serializer;

  @Parameterized.Parameters  
  public static Collection<Object[]> generateParams() {
    List<Object[]> params = new ArrayList<Object[]>();
    //first we want to run tests for all registreted serializers, and then for two network serializers
    //testig for each serializer type has its own index
    for (byte i = 0; i < ORecordSerializerBinary.INSTANCE.getNumberOfSupportedVersions(); i++) {
      params.add(new Object[]{i});
    }
    return params;
  }
  
  public ORecordSerializerBinaryTest(byte serializerIndex){
    if (db != null){
      db.drop();
    }
    db = new ODatabaseDocumentTx("memory:test").create();
    db.createClass("TestClass");
    db.command(new OCommandSQL("create property TestClass.TestEmbedded EMBEDDED")).execute();
    serializer = new ORecordSerializerBinary(serializerIndex);
  }    
  
  @Test
  public void testGetTypedFiledSimple(){    
    ODocument doc = new ODocument();
    Integer setValue = 16;
    doc.setProperty("TestField", setValue);
    byte[] serializedDoc = serializer.toStream(doc, false);
    Integer value = serializer.deserializeFieldFromRoot(serializedDoc, "TestField");
    Assert.assertEquals(setValue, value);
  }
  
  protected static String stringFromBytes(final byte[] bytes, final int offset, final int len) {
    try {
      return new String(bytes, offset, len, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw OException.wrapException(new OSerializationException("Error on string decoding"), e);
    }
  }
  
  protected static String readString(final BytesContainer bytes) {
    final int len = OVarIntSerializer.readAsInteger(bytes);
    final String res = stringFromBytes(bytes.bytes, bytes.offset, len);
    bytes.skip(len);
    return res;
  }
  
  protected static int readInteger(final BytesContainer container) {
    final int value = OIntegerSerializer.INSTANCE.deserializeLiteral(container.bytes, container.offset);
    container.offset += OIntegerSerializer.INT_SIZE;
    return value;
  }
  
  @Test
  public void testGetFieldNamesFromEmbedded(){
    ODocument root = new ODocument();
    ODocument embedded = new ODocument("TestClass");
    Integer setValue = 17;
    embedded.setProperty("TestField", setValue);
    embedded.setProperty("TestField2", "TestValue");        
    
    root.field("TestEmbedded", embedded);
    root.setClassName("TestClass");        
    db.save(root);
    
    byte[] rootBytes = serializer.toStream(root, false);
    
    OResultBinary embeddedBytesViaGet = serializer.deserializeFieldFromRoot(rootBytes, "TestEmbedded");
    Set<String> fieldNames = embeddedBytesViaGet.getPropertyNames();
    Assert.assertTrue(fieldNames.contains("TestField"));
    Assert.assertTrue(fieldNames.contains("TestField2"));
    
    Object propVal = embeddedBytesViaGet.getProperty("TestField");
    Assert.assertEquals(setValue, propVal);
  }
  
  @Test
  public void testGetTypedFieldEmbedded(){
    ODocument root = new ODocument();
    ODocument embedded = new ODocument("TestClass");
    Integer setValue = 17;
    embedded.setProperty("TestField", setValue);          
    
    root.field("TestEmbedded", embedded);
    root.setClassName("TestClass");
        
    db.save(root);    
    
    byte[] rootBytes = serializer.toStream(root, false);
    byte[] embeddedNativeBytes = serializer.toStream(embedded, false);    
    //want to update data pointers because first byte will be removed
    decreasePositionsBy(embeddedNativeBytes, 1, false);
    //skip serializer version
    embeddedNativeBytes = Arrays.copyOfRange(embeddedNativeBytes, 1, embeddedNativeBytes.length);        
        
    OResultBinary embeddedBytesViaGet = serializer.deserializeFieldFromRoot(rootBytes, "TestEmbedded");    
    byte[] deserializedBytes = Arrays.copyOfRange(embeddedBytesViaGet.getBytes(), embeddedBytesViaGet.getOffset(), embeddedBytesViaGet.getOffset() + embeddedBytesViaGet.getFieldLength());
    BytesContainer container = new BytesContainer(deserializedBytes);
    //if by default serializer doesn't store class name then original
    //value embeddedNativeBytes will not have class name in byes so we want to skip them
    if (!serializer.getCurrentSerializer().isSerializingClassNameByDefault() && 
        serializer.getCurrentSerializer().isSerializingClassNameForEmbedded()){      
      int len = OVarIntSerializer.readAsInteger(container);
      container.skip(len);            
    }
    decreasePositionsBy(deserializedBytes, container.offset + embeddedBytesViaGet.getOffset(), true);
    deserializedBytes = Arrays.copyOfRange(deserializedBytes, container.offset, deserializedBytes.length);
    Assert.assertTrue(Arrays.equals(embeddedNativeBytes, deserializedBytes));
  }
  
  @Test
  public void testGetTypedFieldFromEmbedded(){
    ODocument root = new ODocument();
    ODocument embedded = new ODocument("TestClass");
    Integer setValue = 17;
    embedded.setProperty("TestField", setValue);            
    
    root.field("TestEmbedded", embedded);
    root.setClassName("TestClass");
        
    db.save(root);
    
    byte[] rootBytes = serializer.toStream(root, false);
    
    OResultBinary embeddedBytesViaGet = serializer.deserializeFieldFromRoot(rootBytes, "TestEmbedded");    
    
    Integer testValue = serializer.deserializeFieldFromEmbedded(embeddedBytesViaGet.getBytes(), embeddedBytesViaGet.getOffset(), "TestField", rootBytes[0]);
    Assert.assertEquals(setValue, testValue);
  }
  
  @Test
  public void testGetTypedEmbeddedFromEmbedded(){
    ODocument root = new ODocument("TestClass");
    ODocument embedded = new ODocument("TestClass");
    ODocument embeddedLevel2 = new ODocument("TestClass");
    Integer setValue = 17;    
    embeddedLevel2.setProperty("InnerTestFields", setValue);
    embedded.setProperty("TestEmbedded", embeddedLevel2);                       
    
    root.field("TestEmbedded", embedded);    
        
    db.save(root);    
    
    byte[] rootBytes = serializer.toStream(root, false);
    
    OResultBinary embeddedBytesViaGet = serializer.deserializeFieldFromRoot(rootBytes, "TestEmbedded");        
    OResultBinary embeddedLKevel2BytesViaGet = serializer.deserializeFieldFromEmbedded(embeddedBytesViaGet.getBytes(), embeddedBytesViaGet.getOffset(), "TestEmbedded", rootBytes[0]);
    Integer testValue = serializer.deserializeFieldFromEmbedded(embeddedLKevel2BytesViaGet.getBytes(), embeddedLKevel2BytesViaGet.getOffset(), "InnerTestFields", rootBytes[0]);
    Assert.assertEquals(setValue, testValue);
  }
  
  @Test
  public void testGetFieldFromEmbeddedList(){
    ODocument root = new ODocument();
    ODocument embeddedListElement = new ODocument();
    Integer setValue = 19;
    Integer setValue2 = 21;
    embeddedListElement.field("InnerTestFields", setValue);
    
    byte[] rawElementBytes = serializer.toStream(embeddedListElement, false);
    
    List embeddedList = new  ArrayList();
    embeddedList.add(embeddedListElement);
    embeddedList.add(setValue2);
    
    root.field("TestEmbeddedList", embeddedList, OType.EMBEDDEDLIST);
    
    byte[] rootBytes = serializer.toStream(root, false);
    
    List<Object> embeddedListFieldValue = serializer.deserializeFieldFromRoot(rootBytes, "TestEmbeddedList");
    OResultBinary embeddedListElementBytes = (OResultBinary)embeddedListFieldValue.get(0);
    Integer deserializedValue = serializer.deserializeFieldFromEmbedded(embeddedListElementBytes.getBytes(), embeddedListElementBytes.getOffset(), "InnerTestFields", rootBytes[0]);
    Assert.assertEquals(setValue, deserializedValue);
    
    Integer secondtestVal = (Integer)embeddedListFieldValue.get(1);
    Assert.assertEquals(setValue2, secondtestVal);
  }

  @Test
  public void testGetFieldFromEmbeddedMap(){
    ODocument root = new ODocument();    
    Integer setValue = 23;
    Integer setValue2 = 27;
    Map<String, Object> map = new HashMap<>();
    ODocument embeddedListElement = new ODocument();
    embeddedListElement.field("InnerTestFields", setValue);
    map.put("first", embeddedListElement);
    map.put("second", setValue2);
    
    root.field("TestEmbeddedMap", map, OType.EMBEDDEDMAP);
    byte[] rootBytes = serializer.toStream(root, false);
    
    Map deserializedMap = serializer.deserializeFieldFromRoot(rootBytes, "TestEmbeddedMap");
    OResultBinary firstValDeserialized = (OResultBinary)deserializedMap.get("first");
    Integer deserializedValue = serializer.deserializeFieldFromEmbedded(firstValDeserialized.getBytes(), firstValDeserialized.getOffset(), "InnerTestFields", rootBytes[0]);
    Assert.assertEquals(setValue, deserializedValue);
    
    Integer secondDeserializedValue = (Integer)deserializedMap.get("second");
    Assert.assertEquals(setValue2, secondDeserializedValue);
  }

  private void decreasePositionsBy(byte[] recordBytes, int stepSize, boolean isNested) {
    BytesContainer container = new BytesContainer(recordBytes);
    //for root elements skip serializer version
    if (!isNested)
      container.offset++;
    if (serializer.getCurrentSerializer().isSerializingClassNameByDefault() || isNested){
      readString(container);
    }
    int len = 1;
    while (len != 0){
      len = OVarIntSerializer.readAsInteger(container);
      if (len > 0){
        //read field name
        container.offset += len;
        //read data pointer
        int pointer = readInteger(container);
        //shift pointer by start ofset
        pointer -= stepSize;
        //write to byte container
        OIntegerSerializer.INSTANCE.serializeLiteral(pointer, container.bytes, container.offset - OIntegerSerializer.INT_SIZE);
        //read type
        container.offset++;
      }
      else if (len < 0){
        //rtead data pointer
        int pointer = readInteger(container);
        //shift pointer
        pointer -= stepSize;
        //write to byte container
        OIntegerSerializer.INSTANCE.serializeLiteral(pointer, container.bytes, container.offset - OIntegerSerializer.INT_SIZE);
      }
    }
  }
  
}
