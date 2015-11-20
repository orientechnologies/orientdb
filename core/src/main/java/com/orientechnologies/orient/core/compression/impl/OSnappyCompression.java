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

package com.orientechnologies.orient.core.compression.impl;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import org.xerial.snappy.Snappy;

import java.io.IOException;

/**
 * @author Andrey Lomakin
 * @since 05.06.13
 */
public class OSnappyCompression extends OAbstractCompression {
  public static final String             NAME     = "snappy";

  public static final OSnappyCompression INSTANCE = new OSnappyCompression();

  @Override
  public byte[] compress(byte[] content, final int offset, final int length) {
    try {
      final byte[] buf = new byte[Snappy.maxCompressedLength(length)];
      final int compressedByteSize = Snappy.rawCompress(content, offset, length, buf, 0);
      final byte[] result = new byte[compressedByteSize];
      System.arraycopy(buf, 0, result, 0, compressedByteSize);
      return result;
    } catch (IOException e) {
      throw OException.wrapException(new ODatabaseException("Error during data compression"), e);
    }
  }

  @Override
  public byte[] uncompress(byte[] content, final int offset, final int length) {
    try {
      byte[] result = new byte[Snappy.uncompressedLength(content, offset, length)];
      int byteSize = Snappy.uncompress(content, offset, length, result, 0);
      return result;

    } catch (IOException e) {
      throw OException.wrapException(new ODatabaseException("Error during data decompression"), e);
    }
  }

  @Override
  public String name() {
    return NAME;
  }
}
