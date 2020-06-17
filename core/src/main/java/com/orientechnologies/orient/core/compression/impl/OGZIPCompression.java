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

import com.orientechnologies.orient.core.serialization.OMemoryStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * @author Andrey Lomakin
 * @since 05.06.13
 */
public class OGZIPCompression extends OAbstractCompression {
  public static final String NAME = "gzip";

  public static final OGZIPCompression INSTANCE = new OGZIPCompression();

  @Override
  public byte[] compress(final byte[] content, final int offset, final int length) {
    try {
      final byte[] result;
      final OMemoryStream memoryOutputStream = new OMemoryStream();
      final GZIPOutputStream gzipOutputStream =
          new GZIPOutputStream(memoryOutputStream, 16384); // 16KB
      try {
        gzipOutputStream.write(content, offset, length);
        gzipOutputStream.finish();
        result = memoryOutputStream.toByteArray();
      } finally {
        gzipOutputStream.close();
      }

      return result;
    } catch (IOException ioe) {
      throw new IllegalStateException("Exception during data compression", ioe);
    }
  }

  @Override
  public byte[] uncompress(byte[] content, final int offset, final int length) {
    try {
      final ByteArrayInputStream memoryInputStream =
          new ByteArrayInputStream(content, offset, length);
      final GZIPInputStream gzipInputStream = new GZIPInputStream(memoryInputStream, 16384); // 16KB

      try {
        final byte[] buffer = new byte[1024];
        byte[] result = new byte[1024];

        int bytesRead;

        int len = 0;
        while ((bytesRead = gzipInputStream.read(buffer, 0, buffer.length)) > -1) {
          if (len + bytesRead > result.length) {
            int newSize = 2 * result.length;
            if (newSize < len + bytesRead) newSize = Integer.MAX_VALUE;

            final byte[] oldResult = result;
            result = new byte[newSize];
            System.arraycopy(oldResult, 0, result, 0, oldResult.length);
          }

          System.arraycopy(buffer, 0, result, len, bytesRead);
          len += bytesRead;
        }

        return Arrays.copyOf(result, len);

      } finally {
        gzipInputStream.close();
      }

    } catch (IOException ioe) {
      throw new IllegalStateException("Exception during data uncompression", ioe);
    }
  }

  @Override
  public String name() {
    return NAME;
  }
}
