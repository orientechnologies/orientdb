/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */

package com.orientechnologies.orient.core.serialization.serializer.record.binary;

public class BytesContainer {

  public byte[] bytes;
  public int offset;

  public BytesContainer(byte[] iSource) {
    bytes = iSource;
  }

  public BytesContainer() {
    bytes = new byte[64];
  }

  public BytesContainer(final byte[] iBytes, final int iOffset) {
    this.bytes = iBytes;
    this.offset = iOffset;
  }

  public BytesContainer copy() {
    return new BytesContainer(bytes, offset);
  }

  public int alloc(final int toAlloc) {
    final int cur = offset;
    offset += toAlloc;
    if (bytes.length < offset) resize();
    return cur;
  }

  public int allocExact(final int toAlloc) {
    final int cur = offset;
    offset += toAlloc;
    if (bytes.length < offset) {
      byte[] newArray = new byte[offset];
      System.arraycopy(bytes, 0, newArray, 0, bytes.length);
      bytes = newArray;
    }
    return cur;
  }

  public BytesContainer skip(final int read) {
    offset += read;
    return this;
  }

  public byte[] fitBytes() {
    if (bytes.length == offset) {
      return bytes;
    }
    final byte[] fitted = new byte[offset];
    System.arraycopy(bytes, 0, fitted, 0, offset);
    return fitted;
  }

  private void resize() {
    int newLength = bytes.length;
    while (newLength < offset) newLength *= 2;
    final byte[] newBytes = new byte[newLength];
    System.arraycopy(bytes, 0, newBytes, 0, bytes.length);
    bytes = newBytes;
  }
}
