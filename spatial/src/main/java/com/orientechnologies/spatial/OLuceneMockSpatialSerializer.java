/*
 *
 *  * Copyright 2014 Orient Technologies.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *  
 */

package com.orientechnologies.spatial;

import com.orientechnologies.common.directmemory.ODirectMemoryPointer;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALChangesTree;

/**
 * Created by Enrico Risa on 04/09/15.
 */
public class OLuceneMockSpatialSerializer implements OBinarySerializer<ODocument> {


  static OLuceneMockSpatialSerializer INSTANCE = new OLuceneMockSpatialSerializer();

  protected OLuceneMockSpatialSerializer() {

  }

  @Override
  public int getObjectSize(ODocument object, Object... hints) {
    return 0;
  }

  @Override
  public int getObjectSize(byte[] stream, int startPosition) {
    return 0;
  }

  @Override
  public void serialize(ODocument object, byte[] stream, int startPosition, Object... hints) {

  }

  @Override
  public ODocument deserialize(byte[] stream, int startPosition) {
    return null;
  }

  @Override
  public byte getId() {
    return -10;
  }

  @Override
  public boolean isFixedLength() {
    return false;
  }

  @Override
  public int getFixedLength() {
    return 0;
  }

  @Override
  public void serializeNativeObject(ODocument object, byte[] stream, int startPosition, Object... hints) {

  }

  @Override
  public ODocument deserializeNativeObject(byte[] stream, int startPosition) {
    return null;
  }

  @Override
  public int getObjectSizeNative(byte[] stream, int startPosition) {
    return 0;
  }

  @Override
  public void serializeInDirectMemoryObject(ODocument object, ODirectMemoryPointer pointer, long offset, Object... hints) {

  }

  @Override
  public ODocument deserializeFromDirectMemoryObject(ODirectMemoryPointer pointer, long offset) {
    return null;
  }

  @Override
  public int getObjectSizeInDirectMemory(ODirectMemoryPointer pointer, long offset) {
    return 0;
  }

  @Override
  public ODocument deserializeFromDirectMemoryObject(OWALChangesTree.PointerWrapper wrapper, long offset) {
    return null;
  }

  @Override
  public int getObjectSizeInDirectMemory(OWALChangesTree.PointerWrapper wrapper, long offset) {
    return 0;
  }

  @Override
  public ODocument preprocess(ODocument value, Object... hints) {
    return null;
  }
}
