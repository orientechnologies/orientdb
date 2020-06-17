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

package com.orientechnologies.common.hash;

/** @since 13.08.12 */
public class OMurmurHash3 {
  private static class State {
    private long h1;
    private long h2;

    private long k1;
    private long k2;

    private long c1;
    private long c2;
  }

  static long getblock(byte[] key, int i) {
    return (((long) key[i + 0] & 0x00000000000000FFL))
        | (((long) key[i + 1] & 0x00000000000000FFL) << 8)
        | (((long) key[i + 2] & 0x00000000000000FFL) << 16)
        | (((long) key[i + 3] & 0x00000000000000FFL) << 24)
        | (((long) key[i + 4] & 0x00000000000000FFL) << 32)
        | (((long) key[i + 5] & 0x00000000000000FFL) << 40)
        | (((long) key[i + 6] & 0x00000000000000FFL) << 48)
        | (((long) key[i + 7] & 0x00000000000000FFL) << 56);
  }

  static void bmix(State state) {
    state.k1 *= state.c1;
    state.k1 = (state.k1 << 23) | (state.k1 >>> 64 - 23);
    state.k1 *= state.c2;
    state.h1 ^= state.k1;
    state.h1 += state.h2;

    state.h2 = (state.h2 << 41) | (state.h2 >>> 64 - 41);

    state.k2 *= state.c2;
    state.k2 = (state.k2 << 23) | (state.k2 >>> 64 - 23);
    state.k2 *= state.c1;
    state.h2 ^= state.k2;
    state.h2 += state.h1;

    state.h1 = state.h1 * 3 + 0x52dce729;
    state.h2 = state.h2 * 3 + 0x38495ab5;

    state.c1 = state.c1 * 5 + 0x7b7d159c;
    state.c2 = state.c2 * 5 + 0x6bce6396;
  }

  static long fmix(long k) {
    k ^= k >>> 33;
    k *= 0xff51afd7ed558ccdL;
    k ^= k >>> 33;
    k *= 0xc4ceb9fe1a85ec53L;
    k ^= k >>> 33;

    return k;
  }

  public static long murmurHash3_x64_64(final byte[] key, final int seed) {
    State state = new State();

    state.h1 = 0x9368e53c2f6af274L ^ seed;
    state.h2 = 0x586dcd208f7cd3fdL ^ seed;

    state.c1 = 0x87c37b91114253d5L;
    state.c2 = 0x4cf5ad432745937fL;

    for (int i = 0; i < key.length / 16; i++) {
      state.k1 = getblock(key, i * 2 * 8);
      state.k2 = getblock(key, (i * 2 + 1) * 8);

      bmix(state);
    }

    state.k1 = 0;
    state.k2 = 0;

    int tail = (key.length >>> 4) << 4;

    switch (key.length & 15) {
      case 15:
        state.k2 ^= (long) key[tail + 14] << 48;
      case 14:
        state.k2 ^= (long) key[tail + 13] << 40;
      case 13:
        state.k2 ^= (long) key[tail + 12] << 32;
      case 12:
        state.k2 ^= (long) key[tail + 11] << 24;
      case 11:
        state.k2 ^= (long) key[tail + 10] << 16;
      case 10:
        state.k2 ^= (long) key[tail + 9] << 8;
      case 9:
        state.k2 ^= (long) key[tail + 8];

      case 8:
        state.k1 ^= (long) key[tail + 7] << 56;
      case 7:
        state.k1 ^= (long) key[tail + 6] << 48;
      case 6:
        state.k1 ^= (long) key[tail + 5] << 40;
      case 5:
        state.k1 ^= (long) key[tail + 4] << 32;
      case 4:
        state.k1 ^= (long) key[tail + 3] << 24;
      case 3:
        state.k1 ^= (long) key[tail + 2] << 16;
      case 2:
        state.k1 ^= (long) key[tail + 1] << 8;
      case 1:
        state.k1 ^= (long) key[tail + 0];
        bmix(state);
    }

    state.h2 ^= key.length;

    state.h1 += state.h2;
    state.h2 += state.h1;

    state.h1 = fmix(state.h1);
    state.h2 = fmix(state.h2);

    state.h1 += state.h2;
    state.h2 += state.h1;

    return state.h1;
  }
}
