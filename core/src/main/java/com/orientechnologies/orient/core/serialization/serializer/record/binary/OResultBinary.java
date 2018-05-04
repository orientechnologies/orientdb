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
package com.orientechnologies.orient.core.serialization.serializer.record.binary;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.record.impl.OBlob;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 *
 * @author mdjurovi
 */
public class OResultBinary implements OResult{

  private ORID id;
  private final byte[] bytes;
  private final int offset;
  private final int serializerVersion;
  private final int fieldLength;
  
  public OResultBinary(byte[] bytes, int offset, int fieldLength, int serializerVersion){
    this.bytes = bytes;
    this.serializerVersion = serializerVersion;
    this.offset = offset;
    this.fieldLength = fieldLength;
  }

  public int getFieldLength() {
    return fieldLength;
  }

  public int getOffset() {
    return offset;
  }  
  
  public ORID getId() {
    return id;
  }

  public void setId(ORID id) {
    this.id = id;
  }

  public byte[] getBytes() {
    return bytes;
  }

  public int getSerializerVersion() {
    return serializerVersion;
  }    
  
  @Override
  public <T> T getProperty(String name) {
    return (T)ORecordSerializerBinary.INSTANCE.deserializeFieldFromEmbedded(bytes, offset, name, serializerVersion);
  }

  @Override
  public OElement getElementProperty(String name) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public OVertex getVertexProperty(String name) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public OEdge getEdgeProperty(String name) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public OBlob getBlobProperty(String name) {
    return null;
  }

  @Override
  public Set<String> getPropertyNames() {
    String[] fields = ORecordSerializerBinary.INSTANCE.getFieldNamesEmbedded(new ODocument(), bytes, offset, serializerVersion);
    return new HashSet<>(Arrays.asList(fields));
  }

  @Override
  public Optional<ORID> getIdentity() {
    return Optional.of(id);
  }

  @Override
  public boolean isElement() {
    return true;
  }

  @Override
  public Optional<OElement> getElement() {
    return Optional.of(toDocument());
  }

  @Override
  public OElement toElement() {
    return toDocument();
  }

  @Override
  public boolean isBlob() {
    return false;
  }

  @Override
  public Optional<OBlob> getBlob() {
    return null;
  }

  @Override
  public Optional<ORecord> getRecord() {
    return Optional.of(toDocument());
  }

  @Override
  public boolean isProjection() {
    return false;
  }

  @Override
  public Object getMetadata(String key) {
    return null;
  }

  @Override
  public Set<String> getMetadataKeys() {
    return null;
  }

  @Override
  public boolean hasProperty(String varName) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }
  
  private ODocument toDocument(){
    ODocument doc = new ODocument();
    ORecordSerializerBinary.INSTANCE.getSerializer(serializerVersion).deserialize(doc, new BytesContainer(bytes));
    return doc;
  }
  
}
