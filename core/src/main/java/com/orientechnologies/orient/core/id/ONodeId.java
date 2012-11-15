/*
 * Copyright 1999-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.orientechnologies.orient.core.id;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.util.MersenneTwister;

/**
 * Id of the server node in autoshareded storage. It is presented as 192 bit number with values from -2<sup>192</sup>+1 till
 * 2<sup>192</sup>-1.
 * 
 * Internally it presents as unsigned 192 bit number with signature flag.
 * 
 * @author Andrey Lomakin
 * @since 12.11.12
 */
public class ONodeId extends Number implements Comparable<ONodeId> {
  private static final int             CHUNKS_SIZE            = 6;

  public static final int              NODE_SIZE_BYTES        = CHUNKS_SIZE * OIntegerSerializer.INT_SIZE;
  public static final int              NODE_SIZE_BITS         = NODE_SIZE_BYTES * 8;

  private static final long            LONG_INT_MASK          = 0xFFFFFFFFL;

  private static final int             UNSIGNED_INT_MAX_VALUE = 0xFFFFFFFF;

  public static final ONodeId          MAX_VALUE              = new ONodeId(new int[] { UNSIGNED_INT_MAX_VALUE,
      UNSIGNED_INT_MAX_VALUE, UNSIGNED_INT_MAX_VALUE, UNSIGNED_INT_MAX_VALUE, UNSIGNED_INT_MAX_VALUE, UNSIGNED_INT_MAX_VALUE });

  public static final ONodeId          MIN_VALUE              = new ONodeId(new int[CHUNKS_SIZE]);

  public static final ONodeId          ONE                    = new ONodeId(new int[] { 0, 0, 0, 0, 0, 1 });
  public static final ONodeId          ZERO                   = MIN_VALUE;

  private static final MersenneTwister random                 = new MersenneTwister();
  private static byte[]                CURRENT_MAC            = getMac();

  private static final AtomicLong      version                = new AtomicLong();

  static {
    random.setSeed(OLongSerializer.INSTANCE.deserialize(new SecureRandom().generateSeed(OLongSerializer.LONG_SIZE), 0));
  }

  private final int[]                  chunks;

  public ONodeId() {
    this(new int[CHUNKS_SIZE]);
  }

  private ONodeId(int[] chunks) {
    this.chunks = chunks;
  }

  @Override
  public int compareTo(ONodeId o) {
    for (int i = 0; i < CHUNKS_SIZE; i++) {
      final long chunk = chunks[i] & LONG_INT_MASK;
      final long otherChunk = o.chunks[i] & LONG_INT_MASK;

      if (chunk == otherChunk)
        continue;

      if (chunk > otherChunk)
        return 1;

      return -1;
    }

    return 0;
  }

  public ONodeId add(final ONodeId idToAdd) {
    int[] chunksToAdd = idToAdd.chunks;
    int[] result = new int[CHUNKS_SIZE];

    int index = CHUNKS_SIZE;
    long sum = 0;

    while (index > 0) {
      index--;
      sum = (chunks[index] & LONG_INT_MASK) + (chunksToAdd[index] & LONG_INT_MASK) + (sum >>> 32);
      result[index] = (int) sum;
    }

    return new ONodeId(result);
  }

  public ONodeId subtract(final ONodeId idToSubtract) {
    int[] chunksToSubtract = idToSubtract.chunks;
    int[] result = new int[CHUNKS_SIZE];

    int index = CHUNKS_SIZE;
    long difference = 0;

    while (index > 0) {
      index--;
      difference = (chunks[index] & LONG_INT_MASK) - (chunksToSubtract[index] & LONG_INT_MASK) + (difference >> 32);
      result[index] = (int) difference;
    }

    return new ONodeId(result);
  }

  public ONodeId multiply(final int value) {
    final int[] result = new int[CHUNKS_SIZE];

    long carry = 0;
    for (int j = CHUNKS_SIZE - 1; j >= 0; j--) {
      final long product = (chunks[j] & LONG_INT_MASK) * (value & LONG_INT_MASK) + carry;
      result[j] = (int) product;
      carry = product >>> 32;
    }

    return new ONodeId(result);
  }

  public ONodeId shiftLeft(final int shift) {
    int nInts = shift >>> 5;
    final int nBits = shift & 0x1f;
    final int result[] = new int[CHUNKS_SIZE];

    if (nBits != 0) {
      int nBits2 = 32 - nBits;

      int i = nInts;
      int j = 0;

      while (i < CHUNKS_SIZE - 1)
        result[j++] = chunks[i++] << nBits | chunks[i] >>> nBits2;

      result[j] = chunks[i] << nBits;
    } else
      System.arraycopy(chunks, nInts, result, 0, CHUNKS_SIZE - nInts);

    return new ONodeId(result);
  }

  public ONodeId shiftRight(final int shift) {
    int nInts = shift >>> 5;
    int nBits = shift & 0x1f;
    final int result[] = new int[CHUNKS_SIZE];

    if (nBits != 0) {
      int nBits2 = 32 - nBits;

      int i = 0;
      int j = nInts;

      result[j++] = chunks[i] >>> nBits;

      while (j < CHUNKS_SIZE)
        result[j++] = chunks[i++] << nBits2 | chunks[i] >>> nBits;
    } else
      System.arraycopy(chunks, 0, result, nInts, CHUNKS_SIZE - nInts);

    return new ONodeId(result);
  }

  public int intValue() {
    return chunks[CHUNKS_SIZE - 1];
  }

  @Override
  public long longValue() {
    return (chunks[CHUNKS_SIZE - 2] & LONG_INT_MASK) << 32 + chunks[CHUNKS_SIZE - 1] & LONG_INT_MASK;
  }

  @Override
  public float floatValue() {
    return longValue();
  }

  @Override
  public double doubleValue() {
    return longValue();
  }

  public byte[] toStream() {
    final byte[] bytes = new byte[NODE_SIZE_BYTES];

    int pos = 0;
    for (int i = 0; i < CHUNKS_SIZE; i++) {
      OIntegerSerializer.INSTANCE.serialize(chunks[i], bytes, pos);
      pos += OIntegerSerializer.INT_SIZE;
    }

    return bytes;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    ONodeId nodeId = (ONodeId) o;

    if (!Arrays.equals(chunks, nodeId.chunks))
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    return chunks != null ? Arrays.hashCode(chunks) : 0;
  }

  @Override
  public String toString() {
    return "0x" + toHexString();
  }

  public String toHexString() {
    final StringBuilder builder = new StringBuilder();
    for (int chunk : chunks)
      builder.append(String.format("%1$08x", chunk));

    return builder.toString();
  }

  public static ONodeId valueOf(long value) {
    if (value == 0)
      return ZERO;

    if (value == 1)
      return ONE;

    final int[] chunks = new int[CHUNKS_SIZE];
    chunks[5] = (int) (value & LONG_INT_MASK);
    chunks[4] = (int) (value >>> 32);

    return new ONodeId(chunks);
  }

  public static ONodeId valueOf(String value) {
    if (value.length() != NODE_SIZE_BYTES * 2)
      throw new IllegalArgumentException("Invalid length of provided node id should be " + (NODE_SIZE_BYTES * 2));

    final int[] chunks = new int[6];
    for (int i = 0; i < CHUNKS_SIZE; i++) {
      final String chunk = value.substring(i * OIntegerSerializer.INT_SIZE * 2, (i + 1) * OIntegerSerializer.INT_SIZE * 2);

      chunks[i] = (int) Long.parseLong(chunk, 16);
    }

    return new ONodeId(chunks);
  }

  public static ONodeId generateUniqueId() {
    final long clusterPosition = random.nextLong(Long.MAX_VALUE);
    final int[] chunks = new int[CHUNKS_SIZE];
    final long millis = System.currentTimeMillis();
    final long localVersion = version.getAndDecrement();

    chunks[0] = (int) (clusterPosition >>> 32);
    chunks[1] = (int) clusterPosition;

    chunks[2] = (int) (localVersion >>> 32);
    chunks[3] = (int) localVersion;

    chunks[4] = OIntegerSerializer.INSTANCE.deserialize(CURRENT_MAC, 0);
    chunks[5] = ((CURRENT_MAC[4] & 0xFF) << 24) + ((CURRENT_MAC[5] & 0xFF) << 16) + (int) ((millis >>> 3) & 0xFFFF);

    return new ONodeId(chunks);
  }

  private static byte[] getMac() {
    try {
      InetAddress ip = InetAddress.getLocalHost();
      NetworkInterface networkInterface = NetworkInterface.getByInetAddress(ip);
      return networkInterface.getHardwareAddress();
    } catch (UnknownHostException e) {
      throw new IllegalStateException("Error during MAC address retrieval.", e);
    } catch (SocketException e) {
      throw new IllegalStateException("Error during MAC address retrieval.", e);
    }
  }
}
