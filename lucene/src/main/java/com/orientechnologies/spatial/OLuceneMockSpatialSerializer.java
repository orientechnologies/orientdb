/**
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * <p>For more information: http://www.orientdb.com
 */
package com.orientechnologies.spatial;

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALChanges;
import java.nio.ByteBuffer;

/** Created by Enrico Risa on 04/09/15. */
public class OLuceneMockSpatialSerializer implements OBinarySerializer<ODocument> {

  protected static OLuceneMockSpatialSerializer INSTANCE = new OLuceneMockSpatialSerializer();

  protected OLuceneMockSpatialSerializer() {}

  @Override
  public int getObjectSize(ODocument object, Object... hints) {
    return 0;
  }

  @Override
  public int getObjectSize(byte[] stream, int startPosition) {
    return 0;
  }

  @Override
  public void serialize(ODocument object, byte[] stream, int startPosition, Object... hints) {}

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
  public void serializeNativeObject(
      ODocument object, byte[] stream, int startPosition, Object... hints) {}

  @Override
  public ODocument deserializeNativeObject(byte[] stream, int startPosition) {
    return null;
  }

  @Override
  public int getObjectSizeNative(byte[] stream, int startPosition) {
    return 0;
  }

  @Override
  public ODocument preprocess(ODocument value, Object... hints) {
    return null;
  }

  @Override
  public void serializeInByteBufferObject(ODocument object, ByteBuffer buffer, Object... hints) {}

  @Override
  public ODocument deserializeFromByteBufferObject(ByteBuffer buffer) {
    return null;
  }

  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer) {
    return 0;
  }

  @Override
  public ODocument deserializeFromByteBufferObject(
      ByteBuffer buffer, OWALChanges walChanges, int offset) {
    return null;
  }

  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer, OWALChanges walChanges, int offset) {
    return 0;
  }
}
