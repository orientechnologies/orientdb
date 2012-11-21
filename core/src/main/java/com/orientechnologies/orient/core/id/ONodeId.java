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

import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.Arrays;

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

  public static final int              SERIALIZED_SIZE        = NODE_SIZE_BYTES + 1;
  private static final long            LONG_INT_MASK          = 0xFFFFFFFFL;
  private static final int             UNSIGNED_INT_MAX_VALUE = 0xFFFFFFFF;

  public static final ONodeId          MAX_VALUE              = new ONodeId(new int[] { UNSIGNED_INT_MAX_VALUE,
      UNSIGNED_INT_MAX_VALUE, UNSIGNED_INT_MAX_VALUE, UNSIGNED_INT_MAX_VALUE, UNSIGNED_INT_MAX_VALUE, UNSIGNED_INT_MAX_VALUE }, 1);

  public static final ONodeId          MIN_VALUE              = new ONodeId(new int[] { UNSIGNED_INT_MAX_VALUE,
      UNSIGNED_INT_MAX_VALUE, UNSIGNED_INT_MAX_VALUE, UNSIGNED_INT_MAX_VALUE, UNSIGNED_INT_MAX_VALUE, UNSIGNED_INT_MAX_VALUE }, -1);

  public static final ONodeId          ZERO                   = new ONodeId(new int[CHUNKS_SIZE], 0);
  public static final ONodeId          ONE                    = new ONodeId(new int[] { 0, 0, 0, 0, 0, 1 }, 1);
  public static final ONodeId          TWO                    = new ONodeId(new int[] { 0, 0, 0, 0, 0, 2 }, 1);

  private static final MersenneTwister random                 = new MersenneTwister();
  private static final SecureRandom    secureRandom           = new SecureRandom();

  static {
    random.setSeed(OLongSerializer.INSTANCE.deserialize(secureRandom.generateSeed(OLongSerializer.LONG_SIZE), 0));
  }

  private final int[]                  chunks;
  private final int                    signum;

  private ONodeId(int[] chunks, int signum) {
    this.chunks = chunks;
    this.signum = signum;
  }

  @Override
  public int compareTo(ONodeId o) {
    if (signum > o.signum)
      return 1;
    else if (signum < o.signum)
      return -1;
    if (signum == 0 && o.signum == 0)
      return 0;

    final int result = compareChunks(chunks, o.chunks);
    if (signum < 0)
      return -result;

    return result;
  }

  public ONodeId add(final ONodeId idToAdd) {
    if (idToAdd.signum == 0)
      return new ONodeId(chunks, signum);

    if (signum == 0)
      return new ONodeId(idToAdd.chunks, idToAdd.signum);

    final int[] result;
    if (signum == idToAdd.signum) {
      result = addArrays(chunks, idToAdd.chunks);

			if (Arrays.equals(ZERO.chunks, result))
				return ZERO;

      return new ONodeId(result, signum);
    }

    final int cmp = compareChunks(chunks, idToAdd.chunks);
    if (cmp == 0)
      return ZERO;

    if (cmp > 0)
      result = substructArrays(chunks, idToAdd.chunks);
    else
      result = substructArrays(idToAdd.chunks, chunks);

    return new ONodeId(result, cmp == signum ? 1 : -1);
  }

  public ONodeId subtract(final ONodeId idToSubtract) {
    if (idToSubtract.signum == 0)
      return this;

    if (signum == 0)
      return new ONodeId(idToSubtract.chunks, -idToSubtract.signum);

		final int[] result;
    if (signum != idToSubtract.signum) {
			result = addArrays(chunks, idToSubtract.chunks);

			if (Arrays.equals(ZERO.chunks, result))
				return ZERO;

			return new ONodeId(result, signum);
		}

    int cmp = compareChunks(chunks, idToSubtract.chunks);
    if (cmp == 0)
      return ZERO;


    if (cmp > 0)
      result = substructArrays(chunks, idToSubtract.chunks);
    else
      result = substructArrays(idToSubtract.chunks, chunks);

    return new ONodeId(result, cmp == signum ? 1 : -1);
  }

  public ONodeId multiply(final int value) {
    if (value == 0)
      return ZERO;

    final int[] result = new int[CHUNKS_SIZE];

    long carry = 0;
    for (int j = CHUNKS_SIZE - 1; j >= 0; j--) {
      final long product = (chunks[j] & LONG_INT_MASK) * (value & LONG_INT_MASK) + carry;
      result[j] = (int) product;
      carry = product >>> 32;
    }

    return new ONodeId(result, signum);
  }

  public ONodeId shiftLeft(final int shift) {
    int nInts = shift >>> 5;

    if (nInts == CHUNKS_SIZE)
      return ZERO;

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

    if (Arrays.equals(ZERO.chunks, result))
      return ZERO;

    return new ONodeId(result, signum);
  }

  public ONodeId shiftRight(final int shift) {
    int nInts = shift >>> 5;

    if (nInts == CHUNKS_SIZE)
      return ZERO;

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

    if (Arrays.equals(ZERO.chunks, result))
      return ZERO;

    return new ONodeId(result, signum);
  }

  public static ONodeId generateUniqueId() {
    final long clusterPosition = random.nextLong(Long.MAX_VALUE);

    final int[] chunks = new int[CHUNKS_SIZE];
    final byte[] uuid = new byte[16];
    secureRandom.nextBytes(uuid);

    chunks[0] = (int) (clusterPosition >>> 32);
    chunks[1] = (int) clusterPosition;

    chunks[2] = OIntegerSerializer.INSTANCE.deserialize(uuid, 0);
    chunks[3] = OIntegerSerializer.INSTANCE.deserialize(uuid, 4);

    chunks[4] = OIntegerSerializer.INSTANCE.deserialize(uuid, 8);
    chunks[5] = OIntegerSerializer.INSTANCE.deserialize(uuid, 12);

    return new ONodeId(chunks, 1);
  }

  private static int[] addArrays(int[] chunksToAddOne, int[] chunksToAddTwo) {
    int[] result = new int[CHUNKS_SIZE];

    int index = CHUNKS_SIZE;
    long sum = 0;

    while (index > 0) {
      index--;
      sum = (chunksToAddTwo[index] & LONG_INT_MASK) + (chunksToAddOne[index] & LONG_INT_MASK) + (sum >>> 32);
      result[index] = (int) sum;
    }
    return result;
  }

  private static int compareChunks(int[] chunksOne, int[] chunksTwo) {
    for (int i = 0; i < CHUNKS_SIZE; i++) {
      final long chunk = chunksOne[i] & LONG_INT_MASK;
      final long otherChunk = chunksTwo[i] & LONG_INT_MASK;

      if (chunk == otherChunk)
        continue;

      if (chunk > otherChunk)
        return 1;
      return -1;
    }

    return 0;
  }

  private static int[] substructArrays(int[] chunksOne, int[] chunksTwo) {
    int[] result = new int[CHUNKS_SIZE];

    int index = CHUNKS_SIZE;
    long difference = 0;

    while (index > 0) {
      index--;
      difference = (chunksOne[index] & LONG_INT_MASK) - (chunksTwo[index] & LONG_INT_MASK) + (difference >> 32);
      result[index] = (int) difference;
    }
    return result;
  }

  private static void multiplyAndAdd(int[] chunks, int multiplier, int summand) {
    long carry = 0;
    for (int j = CHUNKS_SIZE - 1; j >= 0; j--) {
      final long product = (chunks[j] & LONG_INT_MASK) * (multiplier & LONG_INT_MASK) + carry;
      chunks[j] = (int) product;
      carry = product >>> 32;
    }

    if (summand == 0)
      return;

    long sum = (chunks[CHUNKS_SIZE - 1] & LONG_INT_MASK) + (summand & LONG_INT_MASK);
    chunks[CHUNKS_SIZE - 1] = (int) sum;

    int j = CHUNKS_SIZE - 2;
    while (j >= 0 && sum > 0) {
      sum = (chunks[j] & LONG_INT_MASK) + (sum >>> 32);
      chunks[j] = (int) sum;
      j--;
    }
  }

  public int intValue() {
    return chunks[CHUNKS_SIZE - 1];
  }

  @Override
  public long longValue() {
    return ((chunks[CHUNKS_SIZE - 2] & LONG_INT_MASK) << 32) + (chunks[CHUNKS_SIZE - 1] & LONG_INT_MASK);
  }

  @Override
  public float floatValue() {
    return Float.parseFloat(toString());
  }

  @Override
  public double doubleValue() {
    return Double.parseDouble(toString());
  }

  public byte[] toStream() {
    final byte[] bytes = new byte[SERIALIZED_SIZE];

    int pos = 0;
    for (int i = 0; i < CHUNKS_SIZE; i++) {
      OIntegerSerializer.INSTANCE.serialize(chunks[i], bytes, pos);
      pos += OIntegerSerializer.INT_SIZE;
    }

    bytes[pos] = (byte) signum;

    return bytes;
  }

  public byte[] chunksToByteArray() {
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

    ONodeId oNodeId = (ONodeId) o;

    if (signum != oNodeId.signum)
      return false;
    return Arrays.equals(chunks, oNodeId.chunks);

  }

  @Override
  public int hashCode() {
    int result = Arrays.hashCode(chunks);
    result = 31 * result + signum;
    return result;
  }

  public String toString() {
    return new BigInteger(signum, chunksToByteArray()).toString();
  }

  public static ONodeId valueOf(long value) {
    final ONodeId constant = findInConstantPool(value);
    if (constant != null)
      return constant;

    final int signum;

    if (value > 0)
      signum = 1;
    else {
      signum = -1;
      value = -value;
    }

    final int[] chunks = new int[CHUNKS_SIZE];
    chunks[5] = (int) (value & LONG_INT_MASK);
    chunks[4] = (int) (value >>> 32);

    return new ONodeId(chunks, signum);
  }

  public static ONodeId parseString(String value) {
    final int intChunkLength = 9;
    final int longChunkLength = 18;

    int signum;

    int pos;
    if (value.charAt(0) == '-') {
      pos = 1;
      signum = -1;
    } else {
      pos = 0;
      signum = 1;
    }

    while (pos < value.length() && Character.digit(value.charAt(pos), 10) == 0)
      pos++;

    if (pos == value.length())
      return ZERO;

    int chunkToRead = Math.min(pos + longChunkLength, value.length());
    long initialValue = Long.parseLong(value.substring(pos, chunkToRead));
    pos = chunkToRead;

    int[] result = new int[CHUNKS_SIZE];
    result[CHUNKS_SIZE - 1] = (int) initialValue;
    result[CHUNKS_SIZE - 2] = (int) (initialValue >>> 32);

    while (pos < value.length()) {
      chunkToRead = Math.min(pos + intChunkLength, value.length());
      int parsedValue = Integer.parseInt(value.substring(pos, chunkToRead));
      final int multiplier = (chunkToRead == intChunkLength) ? 1000000000 : (int) Math.pow(10, chunkToRead - pos);
      multiplyAndAdd(result, multiplier, parsedValue);
      pos = chunkToRead;
    }

    return new ONodeId(result, signum);
  }

  public static ONodeId fromStream(byte[] content, int start) {
    final int[] chunks = new int[CHUNKS_SIZE];

    int pos = start;
    for (int i = 0; i < CHUNKS_SIZE; i++) {
      chunks[i] = OIntegerSerializer.INSTANCE.deserialize(content, pos);
      pos += OIntegerSerializer.INT_SIZE;
    }

    final int signum = content[pos];

    return new ONodeId(chunks, signum);
  }

  public static ONodeId parseHexSting(String value) {
    int pos;
		int signum;

    if (value.charAt(0) == '-') {
			pos = 1;
			signum = -1;
		} else {
			pos = 0;
			signum = 1;
		}


    final int[] chunks = new int[6];
    for (int i = 0; i < CHUNKS_SIZE; i++) {
      final String chunk = value.substring(pos, pos + OIntegerSerializer.INT_SIZE * 2);

      chunks[i] = (int) Long.parseLong(chunk, 16);
      pos += OIntegerSerializer.INT_SIZE * 2;
    }

    if (Arrays.equals(ZERO.chunks, chunks))
      return ZERO;

    return new ONodeId(chunks, signum);
  }

  public String toHexString() {
    final StringBuilder builder = new StringBuilder();
    if (signum < 0)
      builder.append("-");

    for (int chunk : chunks)
      builder.append(String.format("%1$08x", chunk));

    return builder.toString();
  }

  private static ONodeId findInConstantPool(long value) {
    if (value == 0)
      return ZERO;

    if (value == 1)
      return ONE;

    if (value == 2)
      return TWO;

    return null;
  }
}
