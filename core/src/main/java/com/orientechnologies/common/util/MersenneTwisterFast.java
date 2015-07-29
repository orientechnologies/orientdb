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

package com.orientechnologies.common.util;

import com.orientechnologies.common.exception.OException;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;

/**
 * <h3>MersenneTwister and MersenneTwisterFast</h3>
 * <p>
 * <b>Version 17</b>, based on version MT199937(99/10/29) of the Mersenne Twister algorithm found at <a
 * href="http://www.math.keio.ac.jp/matumoto/emt.html"> The Mersenne Twister Home Page</a>, with the initialization improved using
 * the new 2002/1/26 initialization algorithm By Sean Luke, October 2004.
 *
 * <p>
 * <b>MersenneTwister</b> is a drop-in subclass replacement for java.util.Random. It is properly synchronized and can be used in a
 * multithreaded environment. On modern VMs such as HotSpot, it is approximately 1/3 slower than java.util.Random.
 *
 * <p>
 * <b>MersenneTwisterFast</b> is not a subclass of java.util.Random. It has the same public methods as Random does, however, and it
 * is algorithmically identical to MersenneTwister. MersenneTwisterFast has hard-code inlined all of its methods directly, and made
 * all of them final (well, the ones of consequence anyway). Further, these methods are <i>not</i> synchronized, so the same
 * MersenneTwisterFast instance cannot be shared by multiple threads. But all this helps MersenneTwisterFast achieve well over twice
 * the speed of MersenneTwister. java.util.Random is about 1/3 slower than MersenneTwisterFast.
 *
 * <h3>About the Mersenne Twister</h3>
 * <p>
 * This is a Java version of the C-program for MT19937: Integer version. The MT19937 algorithm was created by Makoto Matsumoto and
 * Takuji Nishimura, who ask: "When you use this, send an email to: matumoto@math.keio.ac.jp with an appropriate reference to your
 * work". Indicate that this is a translation of their algorithm into Java.
 *
 * <p>
 * <b>Reference. </b> Makato Matsumoto and Takuji Nishimura, "Mersenne Twister: A 623-Dimensionally Equidistributed Uniform
 * Pseudo-Random Number Generator", <i>ACM Transactions on Modeling and. Computer Simulation,</i> Vol. 8, No. 1, January 1998, pp
 * 3--30.
 *
 * <h3>About this Version</h3>
 *
 * <p>
 * <b>Changes since V16:</b> Added nextDouble(includeZero, includeOne) and nextFloat(includeZero, includeOne) to allow for
 * half-open, fully-closed, and fully-open intervals.
 *
 * <p>
 * <b>Changes Since V15:</b> Added serialVersionUID to quiet compiler warnings from Sun's overly verbose compilers as of JDK 1.5.
 *
 * <p>
 * <b>Changes Since V14:</b> made strictfp, with StrictMath.log and StrictMath.sqrt in nextGaussian instead of Math.log and
 * Math.sqrt. This is largely just to be safe, as it presently makes no difference in the speed, correctness, or results of the
 * algorithm.
 *
 * <p>
 * <b>Changes Since V13:</b> clone() method CloneNotSupportedException removed.
 *
 * <p>
 * <b>Changes Since V12:</b> clone() method added.
 *
 * <p>
 * <b>Changes Since V11:</b> stateEquals(...) method added. MersenneTwisterFast is equal to other MersenneTwisterFasts with
 * identical state; likewise MersenneTwister is equal to other MersenneTwister with identical state. This isn't equals(...) because
 * that requires a contract of immutability to compare by value.
 *
 * <p>
 * <b>Changes Since V10:</b> A documentation error suggested that setSeed(int[]) required an int[] array 624 long. In fact, the
 * array can be any non-zero length. The new version also checks for this fact.
 *
 * <p>
 * <b>Changes Since V9:</b> readState(stream) and writeState(stream) provided.
 *
 * <p>
 * <b>Changes Since V8:</b> setSeed(int) was only using the first 28 bits of the seed; it should have been 32 bits. For small-number
 * seeds the behavior is identical.
 *
 * <p>
 * <b>Changes Since V7:</b> A documentation error in MersenneTwisterFast (but not MersenneTwister) stated that nextDouble selects
 * uniformly from the full-open interval [0,1]. It does not. nextDouble's contract is identical across MersenneTwisterFast,
 * MersenneTwister, and java.util.Random, namely, selection in the half-open interval [0,1). That is, 1.0 should not be returned. A
 * similar contract exists in nextFloat.
 *
 * <p>
 * <b>Changes Since V6:</b> License has changed from LGPL to BSD. New timing information to compare against java.util.Random. Recent
 * versions of HotSpot have helped Random increase in speed to the point where it is faster than MersenneTwister but slower than
 * MersenneTwisterFast (which should be the case, as it's a less complex algorithm but is synchronized).
 *
 * <p>
 * <b>Changes Since V5:</b> New empty constructor made to work the same as java.util.Random -- namely, it seeds based on the current
 * time in milliseconds.
 *
 * <p>
 * <b>Changes Since V4:</b> New initialization algorithms. See (see <a
 * href="http://www.math.keio.ac.jp/matumoto/MT2002/emt19937ar.html"></a>
 * http://www.math.keio.ac.jp/matumoto/MT2002/emt19937ar.html</a>)
 *
 * <p>
 * The MersenneTwister code is based on standard MT19937 C/C++ code by Takuji Nishimura, with suggestions from Topher Cooper and
 * Marc Rieffel, July 1997. The code was originally translated into Java by Michael Lecuyer, January 1999, and the original code is
 * Copyright (c) 1999 by Michael Lecuyer.
 *
 * <h3>Java notes</h3>
 *
 * <p>
 * This implementation implements the bug fixes made in Java 1.2's version of Random, which means it can be used with earlier
 * versions of Java. See <a href="http://www.javasoft.com/products/jdk/1.2/docs/api/java/util/Random.html"> the JDK 1.2
 * java.util.Random documentation</a> for further documentation on the random-number generation contracts made. Additionally,
 * there's an undocumented bug in the JDK java.util.Random.nextBytes() method, which this code fixes.
 *
 * <p>
 * Just like java.util.Random, this generator accepts a long seed but doesn't use all of it. java.util.Random uses 48 bits. The
 * Mersenne Twister instead uses 32 bits (int size). So it's best if your seed does not exceed the int range.
 *
 * <p>
 * MersenneTwister can be used reliably on JDK version 1.1.5 or above. Earlier Java versions have serious bugs in java.util.Random;
 * only MersenneTwisterFast (and not MersenneTwister nor java.util.Random) should be used with them.
 *
 * <h3>License</h3>
 *
 * Copyright (c) 2003 by Sean Luke. <br>
 * Portions copyright (c) 1993 by Michael Lecuyer. <br>
 * All rights reserved. <br>
 *
 * <p>
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following
 * conditions are met:
 * <ul>
 * <li>Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
 * <li>Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following
 * disclaimer in the documentation and/or other materials provided with the distribution.
 * <li>Neither the name of the copyright owners, their employers, nor the names of its contributors may be used to endorse or
 * promote products derived from this software without specific prior written permission.
 * </ul>
 * <p>
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT
 * NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 * THE COPYRIGHT OWNERS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
 * OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * @version 17
 */

// Note: this class is hard-inlined in all of its methods. This makes some of
// the methods well-nigh unreadable in their complexity. In fact, the Mersenne
// Twister is fairly easy code to understand: if you're trying to get a handle
// on the code, I strongly suggest looking at MersenneTwister.java first.
// -- Sean
public class MersenneTwisterFast implements Serializable, Cloneable {
  // Serialization
  private static final long serialVersionUID = -8219700664442619525L; // locked as of Version 15

  // Period parameters
  private static final int  N                = 624;
  private static final int  M                = 397;
  private static final int  MATRIX_A         = 0x9908b0df;           // private static final * constant vector a
  private static final int  UPPER_MASK       = 0x80000000;           // most significant w-r bits
  private static final int  LOWER_MASK       = 0x7fffffff;           // least significant r bits

  // Tempering parameters
  private static final int  TEMPERING_MASK_B = 0x9d2c5680;
  private static final int  TEMPERING_MASK_C = 0xefc60000;

  private int               mt[];                                    // the array for the state vector
  private int               mti;                                     // mti==N+1 means mt[N] is not initialized
  private int               mag01[];

  // a good initial seed (of int size, though stored in a long)
  // private static final long GOOD_SEED = 4357;

  private double            __nextNextGaussian;
  private boolean           __haveNextNextGaussian;

  /**
   * Constructor using the default seed.
   */
  public MersenneTwisterFast() {
    this(System.currentTimeMillis());
  }

  /**
   * Constructor using a given seed. Though you pass this seed in as a long, it's best to make sure it's actually an integer.
   *
   */
  public MersenneTwisterFast(final long seed) {
    setSeed(seed);
  }

  /**
   * Constructor using an array of integers as seed. Your array must have a non-zero length. Only the first 624 integers in the
   * array are used; if the array is shorter than this then integers are repeatedly used in a wrap-around fashion.
   */
  public MersenneTwisterFast(final int[] array) {
    setSeed(array);
  }

  /* We're overriding all internal data, to my knowledge, so this should be okay */
  public Object clone() {
    try {
      MersenneTwisterFast f = (MersenneTwisterFast) (super.clone());
      f.mt = (int[]) (mt.clone());
      f.mag01 = (int[]) (mag01.clone());
      return f;
    } catch (CloneNotSupportedException e) {
      throw new OException(e);
    } // should never happen
  }

  public boolean stateEquals(Object o) {
    if (o == this)
      return true;
    if (o == null || !(o instanceof MersenneTwisterFast))
      return false;
    MersenneTwisterFast other = (MersenneTwisterFast) o;
    if (mti != other.mti)
      return false;
    for (int x = 0; x < mag01.length; x++)
      if (mag01[x] != other.mag01[x])
        return false;
    for (int x = 0; x < mt.length; x++)
      if (mt[x] != other.mt[x])
        return false;
    return true;
  }

  /** Reads the entire state of the MersenneTwister RNG from the stream */
  public void readState(DataInputStream stream) throws IOException {
    int len = mt.length;
    for (int x = 0; x < len; x++)
      mt[x] = stream.readInt();

    len = mag01.length;
    for (int x = 0; x < len; x++)
      mag01[x] = stream.readInt();

    mti = stream.readInt();
    __nextNextGaussian = stream.readDouble();
    __haveNextNextGaussian = stream.readBoolean();
  }

  /** Writes the entire state of the MersenneTwister RNG to the stream */
  public void writeState(DataOutputStream stream) throws IOException {
    int len = mt.length;
    for (int x = 0; x < len; x++)
      stream.writeInt(mt[x]);

    len = mag01.length;
    for (int x = 0; x < len; x++)
      stream.writeInt(mag01[x]);

    stream.writeInt(mti);
    stream.writeDouble(__nextNextGaussian);
    stream.writeBoolean(__haveNextNextGaussian);
  }

  /**
   * Initalize the pseudo random number generator. Don't pass in a long that's bigger than an int (Mersenne Twister only uses the
   * first 32 bits for its seed).
   */

  synchronized public void setSeed(final long seed) {
    // Due to a bug in java.util.Random clear up to 1.2, we're
    // doing our own Gaussian variable.
    __haveNextNextGaussian = false;

    mt = new int[N];

    mag01 = new int[2];
    mag01[0] = 0x0;
    mag01[1] = MATRIX_A;

    mt[0] = (int) (seed & 0xffffffff);
    for (mti = 1; mti < N; mti++) {
      mt[mti] = (1812433253 * (mt[mti - 1] ^ (mt[mti - 1] >>> 30)) + mti);
      /* See Knuth TAOCP Vol2. 3rd Ed. P.106 for multiplier. */
      /* In the previous versions, MSBs of the seed affect */
      /* only MSBs of the array mt[]. */
      /* 2002/01/09 modified by Makoto Matsumoto */
      mt[mti] &= 0xffffffff;
      /* for >32 bit machines */
    }
  }

  /**
   * Sets the seed of the MersenneTwister using an array of integers. Your array must have a non-zero length. Only the first 624
   * integers in the array are used; if the array is shorter than this then integers are repeatedly used in a wrap-around fashion.
   */

  synchronized public void setSeed(final int[] array) {
    if (array.length == 0)
      throw new IllegalArgumentException("Array length must be greater than zero");
    int i, j, k;
    setSeed(19650218);
    i = 1;
    j = 0;
    k = (N > array.length ? N : array.length);
    for (; k != 0; k--) {
      mt[i] = (mt[i] ^ ((mt[i - 1] ^ (mt[i - 1] >>> 30)) * 1664525)) + array[j] + j; /* non linear */
      mt[i] &= 0xffffffff; /* for WORDSIZE > 32 machines */
      i++;
      j++;
      if (i >= N) {
        mt[0] = mt[N - 1];
        i = 1;
      }
      if (j >= array.length)
        j = 0;
    }
    for (k = N - 1; k != 0; k--) {
      mt[i] = (mt[i] ^ ((mt[i - 1] ^ (mt[i - 1] >>> 30)) * 1566083941)) - i; /* non linear */
      mt[i] &= 0xffffffff; /* for WORDSIZE > 32 machines */
      i++;
      if (i >= N) {
        mt[0] = mt[N - 1];
        i = 1;
      }
    }
    mt[0] = 0x80000000; /* MSB is 1; assuring non-zero initial array */
  }

  public final int nextInt() {
    int y;

    if (mti >= N) // generate N words at one time
    {
      int kk;
      final int[] mt = this.mt; // locals are slightly faster
      final int[] mag01 = this.mag01; // locals are slightly faster

      for (kk = 0; kk < N - M; kk++) {
        y = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
        mt[kk] = mt[kk + M] ^ (y >>> 1) ^ mag01[y & 0x1];
      }
      for (; kk < N - 1; kk++) {
        y = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
        mt[kk] = mt[kk + (M - N)] ^ (y >>> 1) ^ mag01[y & 0x1];
      }
      y = (mt[N - 1] & UPPER_MASK) | (mt[0] & LOWER_MASK);
      mt[N - 1] = mt[M - 1] ^ (y >>> 1) ^ mag01[y & 0x1];

      mti = 0;
    }

    y = mt[mti++];
    y ^= y >>> 11; // TEMPERING_SHIFT_U(y)
    y ^= (y << 7) & TEMPERING_MASK_B; // TEMPERING_SHIFT_S(y)
    y ^= (y << 15) & TEMPERING_MASK_C; // TEMPERING_SHIFT_T(y)
    y ^= (y >>> 18); // TEMPERING_SHIFT_L(y)

    return y;
  }

  public final short nextShort() {
    int y;

    if (mti >= N) // generate N words at one time
    {
      int kk;
      final int[] mt = this.mt; // locals are slightly faster
      final int[] mag01 = this.mag01; // locals are slightly faster

      for (kk = 0; kk < N - M; kk++) {
        y = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
        mt[kk] = mt[kk + M] ^ (y >>> 1) ^ mag01[y & 0x1];
      }
      for (; kk < N - 1; kk++) {
        y = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
        mt[kk] = mt[kk + (M - N)] ^ (y >>> 1) ^ mag01[y & 0x1];
      }
      y = (mt[N - 1] & UPPER_MASK) | (mt[0] & LOWER_MASK);
      mt[N - 1] = mt[M - 1] ^ (y >>> 1) ^ mag01[y & 0x1];

      mti = 0;
    }

    y = mt[mti++];
    y ^= y >>> 11; // TEMPERING_SHIFT_U(y)
    y ^= (y << 7) & TEMPERING_MASK_B; // TEMPERING_SHIFT_S(y)
    y ^= (y << 15) & TEMPERING_MASK_C; // TEMPERING_SHIFT_T(y)
    y ^= (y >>> 18); // TEMPERING_SHIFT_L(y)

    return (short) (y >>> 16);
  }

  public final char nextChar() {
    int y;

    if (mti >= N) // generate N words at one time
    {
      int kk;
      final int[] mt = this.mt; // locals are slightly faster
      final int[] mag01 = this.mag01; // locals are slightly faster

      for (kk = 0; kk < N - M; kk++) {
        y = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
        mt[kk] = mt[kk + M] ^ (y >>> 1) ^ mag01[y & 0x1];
      }
      for (; kk < N - 1; kk++) {
        y = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
        mt[kk] = mt[kk + (M - N)] ^ (y >>> 1) ^ mag01[y & 0x1];
      }
      y = (mt[N - 1] & UPPER_MASK) | (mt[0] & LOWER_MASK);
      mt[N - 1] = mt[M - 1] ^ (y >>> 1) ^ mag01[y & 0x1];

      mti = 0;
    }

    y = mt[mti++];
    y ^= y >>> 11; // TEMPERING_SHIFT_U(y)
    y ^= (y << 7) & TEMPERING_MASK_B; // TEMPERING_SHIFT_S(y)
    y ^= (y << 15) & TEMPERING_MASK_C; // TEMPERING_SHIFT_T(y)
    y ^= (y >>> 18); // TEMPERING_SHIFT_L(y)

    return (char) (y >>> 16);
  }

  public final boolean nextBoolean() {
    int y;

    if (mti >= N) // generate N words at one time
    {
      int kk;
      final int[] mt = this.mt; // locals are slightly faster
      final int[] mag01 = this.mag01; // locals are slightly faster

      for (kk = 0; kk < N - M; kk++) {
        y = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
        mt[kk] = mt[kk + M] ^ (y >>> 1) ^ mag01[y & 0x1];
      }
      for (; kk < N - 1; kk++) {
        y = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
        mt[kk] = mt[kk + (M - N)] ^ (y >>> 1) ^ mag01[y & 0x1];
      }
      y = (mt[N - 1] & UPPER_MASK) | (mt[0] & LOWER_MASK);
      mt[N - 1] = mt[M - 1] ^ (y >>> 1) ^ mag01[y & 0x1];

      mti = 0;
    }

    y = mt[mti++];
    y ^= y >>> 11; // TEMPERING_SHIFT_U(y)
    y ^= (y << 7) & TEMPERING_MASK_B; // TEMPERING_SHIFT_S(y)
    y ^= (y << 15) & TEMPERING_MASK_C; // TEMPERING_SHIFT_T(y)
    y ^= (y >>> 18); // TEMPERING_SHIFT_L(y)

    return (boolean) ((y >>> 31) != 0);
  }

  /**
   * This generates a coin flip with a probability <tt>probability</tt> of returning true, else returning false.
   * <tt>probability</tt> must be between 0.0 and 1.0, inclusive. Not as precise a random real event as nextBoolean(double), but
   * twice as fast. To explicitly use this, remember you may need to cast to float first.
   */

  public final boolean nextBoolean(final float probability) {
    int y;

    if (probability < 0.0f || probability > 1.0f)
      throw new IllegalArgumentException("probability must be between 0.0 and 1.0 inclusive.");
    if (probability == 0.0f)
      return false; // fix half-open issues
    else if (probability == 1.0f)
      return true; // fix half-open issues
    if (mti >= N) // generate N words at one time
    {
      int kk;
      final int[] mt = this.mt; // locals are slightly faster
      final int[] mag01 = this.mag01; // locals are slightly faster

      for (kk = 0; kk < N - M; kk++) {
        y = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
        mt[kk] = mt[kk + M] ^ (y >>> 1) ^ mag01[y & 0x1];
      }
      for (; kk < N - 1; kk++) {
        y = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
        mt[kk] = mt[kk + (M - N)] ^ (y >>> 1) ^ mag01[y & 0x1];
      }
      y = (mt[N - 1] & UPPER_MASK) | (mt[0] & LOWER_MASK);
      mt[N - 1] = mt[M - 1] ^ (y >>> 1) ^ mag01[y & 0x1];

      mti = 0;
    }

    y = mt[mti++];
    y ^= y >>> 11; // TEMPERING_SHIFT_U(y)
    y ^= (y << 7) & TEMPERING_MASK_B; // TEMPERING_SHIFT_S(y)
    y ^= (y << 15) & TEMPERING_MASK_C; // TEMPERING_SHIFT_T(y)
    y ^= (y >>> 18); // TEMPERING_SHIFT_L(y)

    return (y >>> 8) / ((float) (1 << 24)) < probability;
  }

  /**
   * This generates a coin flip with a probability <tt>probability</tt> of returning true, else returning false.
   * <tt>probability</tt> must be between 0.0 and 1.0, inclusive.
   */

  public final boolean nextBoolean(final double probability) {
    int y;
    int z;

    if (probability < 0.0 || probability > 1.0)
      throw new IllegalArgumentException("probability must be between 0.0 and 1.0 inclusive.");
    if (probability == 0.0)
      return false; // fix half-open issues
    else if (probability == 1.0)
      return true; // fix half-open issues
    if (mti >= N) // generate N words at one time
    {
      int kk;
      final int[] mt = this.mt; // locals are slightly faster
      final int[] mag01 = this.mag01; // locals are slightly faster

      for (kk = 0; kk < N - M; kk++) {
        y = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
        mt[kk] = mt[kk + M] ^ (y >>> 1) ^ mag01[y & 0x1];
      }
      for (; kk < N - 1; kk++) {
        y = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
        mt[kk] = mt[kk + (M - N)] ^ (y >>> 1) ^ mag01[y & 0x1];
      }
      y = (mt[N - 1] & UPPER_MASK) | (mt[0] & LOWER_MASK);
      mt[N - 1] = mt[M - 1] ^ (y >>> 1) ^ mag01[y & 0x1];

      mti = 0;
    }

    y = mt[mti++];
    y ^= y >>> 11; // TEMPERING_SHIFT_U(y)
    y ^= (y << 7) & TEMPERING_MASK_B; // TEMPERING_SHIFT_S(y)
    y ^= (y << 15) & TEMPERING_MASK_C; // TEMPERING_SHIFT_T(y)
    y ^= (y >>> 18); // TEMPERING_SHIFT_L(y)

    if (mti >= N) // generate N words at one time
    {
      int kk;
      final int[] mt = this.mt; // locals are slightly faster
      final int[] mag01 = this.mag01; // locals are slightly faster

      for (kk = 0; kk < N - M; kk++) {
        z = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
        mt[kk] = mt[kk + M] ^ (z >>> 1) ^ mag01[z & 0x1];
      }
      for (; kk < N - 1; kk++) {
        z = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
        mt[kk] = mt[kk + (M - N)] ^ (z >>> 1) ^ mag01[z & 0x1];
      }
      z = (mt[N - 1] & UPPER_MASK) | (mt[0] & LOWER_MASK);
      mt[N - 1] = mt[M - 1] ^ (z >>> 1) ^ mag01[z & 0x1];

      mti = 0;
    }

    z = mt[mti++];
    z ^= z >>> 11; // TEMPERING_SHIFT_U(z)
    z ^= (z << 7) & TEMPERING_MASK_B; // TEMPERING_SHIFT_S(z)
    z ^= (z << 15) & TEMPERING_MASK_C; // TEMPERING_SHIFT_T(z)
    z ^= (z >>> 18); // TEMPERING_SHIFT_L(z)

    /* derived from nextDouble documentation in jdk 1.2 docs, see top */
    return ((((long) (y >>> 6)) << 27) + (z >>> 5)) / (double) (1L << 53) < probability;
  }

  public final byte nextByte() {
    int y;

    if (mti >= N) // generate N words at one time
    {
      int kk;
      final int[] mt = this.mt; // locals are slightly faster
      final int[] mag01 = this.mag01; // locals are slightly faster

      for (kk = 0; kk < N - M; kk++) {
        y = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
        mt[kk] = mt[kk + M] ^ (y >>> 1) ^ mag01[y & 0x1];
      }
      for (; kk < N - 1; kk++) {
        y = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
        mt[kk] = mt[kk + (M - N)] ^ (y >>> 1) ^ mag01[y & 0x1];
      }
      y = (mt[N - 1] & UPPER_MASK) | (mt[0] & LOWER_MASK);
      mt[N - 1] = mt[M - 1] ^ (y >>> 1) ^ mag01[y & 0x1];

      mti = 0;
    }

    y = mt[mti++];
    y ^= y >>> 11; // TEMPERING_SHIFT_U(y)
    y ^= (y << 7) & TEMPERING_MASK_B; // TEMPERING_SHIFT_S(y)
    y ^= (y << 15) & TEMPERING_MASK_C; // TEMPERING_SHIFT_T(y)
    y ^= (y >>> 18); // TEMPERING_SHIFT_L(y)

    return (byte) (y >>> 24);
  }

  public final void nextBytes(byte[] bytes) {
    int y;

    for (int x = 0; x < bytes.length; x++) {
      if (mti >= N) // generate N words at one time
      {
        int kk;
        final int[] mt = this.mt; // locals are slightly faster
        final int[] mag01 = this.mag01; // locals are slightly faster

        for (kk = 0; kk < N - M; kk++) {
          y = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
          mt[kk] = mt[kk + M] ^ (y >>> 1) ^ mag01[y & 0x1];
        }
        for (; kk < N - 1; kk++) {
          y = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
          mt[kk] = mt[kk + (M - N)] ^ (y >>> 1) ^ mag01[y & 0x1];
        }
        y = (mt[N - 1] & UPPER_MASK) | (mt[0] & LOWER_MASK);
        mt[N - 1] = mt[M - 1] ^ (y >>> 1) ^ mag01[y & 0x1];

        mti = 0;
      }

      y = mt[mti++];
      y ^= y >>> 11; // TEMPERING_SHIFT_U(y)
      y ^= (y << 7) & TEMPERING_MASK_B; // TEMPERING_SHIFT_S(y)
      y ^= (y << 15) & TEMPERING_MASK_C; // TEMPERING_SHIFT_T(y)
      y ^= (y >>> 18); // TEMPERING_SHIFT_L(y)

      bytes[x] = (byte) (y >>> 24);
    }
  }

  public final long nextLong() {
    int y;
    int z;

    if (mti >= N) // generate N words at one time
    {
      int kk;
      final int[] mt = this.mt; // locals are slightly faster
      final int[] mag01 = this.mag01; // locals are slightly faster

      for (kk = 0; kk < N - M; kk++) {
        y = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
        mt[kk] = mt[kk + M] ^ (y >>> 1) ^ mag01[y & 0x1];
      }
      for (; kk < N - 1; kk++) {
        y = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
        mt[kk] = mt[kk + (M - N)] ^ (y >>> 1) ^ mag01[y & 0x1];
      }
      y = (mt[N - 1] & UPPER_MASK) | (mt[0] & LOWER_MASK);
      mt[N - 1] = mt[M - 1] ^ (y >>> 1) ^ mag01[y & 0x1];

      mti = 0;
    }

    y = mt[mti++];
    y ^= y >>> 11; // TEMPERING_SHIFT_U(y)
    y ^= (y << 7) & TEMPERING_MASK_B; // TEMPERING_SHIFT_S(y)
    y ^= (y << 15) & TEMPERING_MASK_C; // TEMPERING_SHIFT_T(y)
    y ^= (y >>> 18); // TEMPERING_SHIFT_L(y)

    if (mti >= N) // generate N words at one time
    {
      int kk;
      final int[] mt = this.mt; // locals are slightly faster
      final int[] mag01 = this.mag01; // locals are slightly faster

      for (kk = 0; kk < N - M; kk++) {
        z = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
        mt[kk] = mt[kk + M] ^ (z >>> 1) ^ mag01[z & 0x1];
      }
      for (; kk < N - 1; kk++) {
        z = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
        mt[kk] = mt[kk + (M - N)] ^ (z >>> 1) ^ mag01[z & 0x1];
      }
      z = (mt[N - 1] & UPPER_MASK) | (mt[0] & LOWER_MASK);
      mt[N - 1] = mt[M - 1] ^ (z >>> 1) ^ mag01[z & 0x1];

      mti = 0;
    }

    z = mt[mti++];
    z ^= z >>> 11; // TEMPERING_SHIFT_U(z)
    z ^= (z << 7) & TEMPERING_MASK_B; // TEMPERING_SHIFT_S(z)
    z ^= (z << 15) & TEMPERING_MASK_C; // TEMPERING_SHIFT_T(z)
    z ^= (z >>> 18); // TEMPERING_SHIFT_L(z)

    return (((long) y) << 32) + (long) z;
  }

  /**
   * Returns a long drawn uniformly from 0 to n-1. Suffice it to say, n must be > 0, or an IllegalArgumentException is raised.
   */
  public final long nextLong(final long n) {
    if (n <= 0)
      throw new IllegalArgumentException("n must be positive, got: " + n);

    long bits, val;
    do {
      int y;
      int z;

      if (mti >= N) // generate N words at one time
      {
        int kk;
        final int[] mt = this.mt; // locals are slightly faster
        final int[] mag01 = this.mag01; // locals are slightly faster

        for (kk = 0; kk < N - M; kk++) {
          y = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
          mt[kk] = mt[kk + M] ^ (y >>> 1) ^ mag01[y & 0x1];
        }
        for (; kk < N - 1; kk++) {
          y = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
          mt[kk] = mt[kk + (M - N)] ^ (y >>> 1) ^ mag01[y & 0x1];
        }
        y = (mt[N - 1] & UPPER_MASK) | (mt[0] & LOWER_MASK);
        mt[N - 1] = mt[M - 1] ^ (y >>> 1) ^ mag01[y & 0x1];

        mti = 0;
      }

      y = mt[mti++];
      y ^= y >>> 11; // TEMPERING_SHIFT_U(y)
      y ^= (y << 7) & TEMPERING_MASK_B; // TEMPERING_SHIFT_S(y)
      y ^= (y << 15) & TEMPERING_MASK_C; // TEMPERING_SHIFT_T(y)
      y ^= (y >>> 18); // TEMPERING_SHIFT_L(y)

      if (mti >= N) // generate N words at one time
      {
        int kk;
        final int[] mt = this.mt; // locals are slightly faster
        final int[] mag01 = this.mag01; // locals are slightly faster

        for (kk = 0; kk < N - M; kk++) {
          z = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
          mt[kk] = mt[kk + M] ^ (z >>> 1) ^ mag01[z & 0x1];
        }
        for (; kk < N - 1; kk++) {
          z = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
          mt[kk] = mt[kk + (M - N)] ^ (z >>> 1) ^ mag01[z & 0x1];
        }
        z = (mt[N - 1] & UPPER_MASK) | (mt[0] & LOWER_MASK);
        mt[N - 1] = mt[M - 1] ^ (z >>> 1) ^ mag01[z & 0x1];

        mti = 0;
      }

      z = mt[mti++];
      z ^= z >>> 11; // TEMPERING_SHIFT_U(z)
      z ^= (z << 7) & TEMPERING_MASK_B; // TEMPERING_SHIFT_S(z)
      z ^= (z << 15) & TEMPERING_MASK_C; // TEMPERING_SHIFT_T(z)
      z ^= (z >>> 18); // TEMPERING_SHIFT_L(z)

      bits = (((((long) y) << 32) + (long) z) >>> 1);
      val = bits % n;
    } while (bits - val + (n - 1) < 0);
    return val;
  }

  /**
   * Returns a random double in the half-open range from [0.0,1.0). Thus 0.0 is a valid result but 1.0 is not.
   */
  public final double nextDouble() {
    int y;
    int z;

    if (mti >= N) // generate N words at one time
    {
      int kk;
      final int[] mt = this.mt; // locals are slightly faster
      final int[] mag01 = this.mag01; // locals are slightly faster

      for (kk = 0; kk < N - M; kk++) {
        y = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
        mt[kk] = mt[kk + M] ^ (y >>> 1) ^ mag01[y & 0x1];
      }
      for (; kk < N - 1; kk++) {
        y = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
        mt[kk] = mt[kk + (M - N)] ^ (y >>> 1) ^ mag01[y & 0x1];
      }
      y = (mt[N - 1] & UPPER_MASK) | (mt[0] & LOWER_MASK);
      mt[N - 1] = mt[M - 1] ^ (y >>> 1) ^ mag01[y & 0x1];

      mti = 0;
    }

    y = mt[mti++];
    y ^= y >>> 11; // TEMPERING_SHIFT_U(y)
    y ^= (y << 7) & TEMPERING_MASK_B; // TEMPERING_SHIFT_S(y)
    y ^= (y << 15) & TEMPERING_MASK_C; // TEMPERING_SHIFT_T(y)
    y ^= (y >>> 18); // TEMPERING_SHIFT_L(y)

    if (mti >= N) // generate N words at one time
    {
      int kk;
      final int[] mt = this.mt; // locals are slightly faster
      final int[] mag01 = this.mag01; // locals are slightly faster

      for (kk = 0; kk < N - M; kk++) {
        z = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
        mt[kk] = mt[kk + M] ^ (z >>> 1) ^ mag01[z & 0x1];
      }
      for (; kk < N - 1; kk++) {
        z = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
        mt[kk] = mt[kk + (M - N)] ^ (z >>> 1) ^ mag01[z & 0x1];
      }
      z = (mt[N - 1] & UPPER_MASK) | (mt[0] & LOWER_MASK);
      mt[N - 1] = mt[M - 1] ^ (z >>> 1) ^ mag01[z & 0x1];

      mti = 0;
    }

    z = mt[mti++];
    z ^= z >>> 11; // TEMPERING_SHIFT_U(z)
    z ^= (z << 7) & TEMPERING_MASK_B; // TEMPERING_SHIFT_S(z)
    z ^= (z << 15) & TEMPERING_MASK_C; // TEMPERING_SHIFT_T(z)
    z ^= (z >>> 18); // TEMPERING_SHIFT_L(z)

    /* derived from nextDouble documentation in jdk 1.2 docs, see top */
    return ((((long) (y >>> 6)) << 27) + (z >>> 5)) / (double) (1L << 53);
  }

  /**
   * Returns a double in the range from 0.0 to 1.0, possibly inclusive of 0.0 and 1.0 themselves. Thus:
   * 
   * <p>
   * <table border=0>
   * <th>
   * <td>Expression
   * <td>Interval
   * <tr>
   * <td>nextDouble(false, false)
   * <td>(0.0, 1.0)
   * <tr>
   * <td>nextDouble(true, false)
   * <td>[0.0, 1.0)
   * <tr>
   * <td>nextDouble(false, true)
   * <td>(0.0, 1.0]
   * <tr>
   * <td>nextDouble(true, true)
   * <td>[0.0, 1.0]
   * </table>
   * 
   * <p>
   * This version preserves all possible random values in the double range.
   */
  public double nextDouble(boolean includeZero, boolean includeOne) {
    double d = 0.0;
    do {
      d = nextDouble(); // grab a value, initially from half-open [0.0, 1.0)
      if (includeOne && nextBoolean())
        d += 1.0; // if includeOne, with 1/2 probability, push to [1.0, 2.0)
    } while ((d > 1.0) || // everything above 1.0 is always invalid
        (!includeZero && d == 0.0)); // if we're not including zero, 0.0 is invalid
    return d;
  }

  public final double nextGaussian() {
    if (__haveNextNextGaussian) {
      __haveNextNextGaussian = false;
      return __nextNextGaussian;
    } else {
      double v1, v2, s;
      do {
        int y;
        int z;
        int a;
        int b;

        if (mti >= N) // generate N words at one time
        {
          int kk;
          final int[] mt = this.mt; // locals are slightly faster
          final int[] mag01 = this.mag01; // locals are slightly faster

          for (kk = 0; kk < N - M; kk++) {
            y = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
            mt[kk] = mt[kk + M] ^ (y >>> 1) ^ mag01[y & 0x1];
          }
          for (; kk < N - 1; kk++) {
            y = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
            mt[kk] = mt[kk + (M - N)] ^ (y >>> 1) ^ mag01[y & 0x1];
          }
          y = (mt[N - 1] & UPPER_MASK) | (mt[0] & LOWER_MASK);
          mt[N - 1] = mt[M - 1] ^ (y >>> 1) ^ mag01[y & 0x1];

          mti = 0;
        }

        y = mt[mti++];
        y ^= y >>> 11; // TEMPERING_SHIFT_U(y)
        y ^= (y << 7) & TEMPERING_MASK_B; // TEMPERING_SHIFT_S(y)
        y ^= (y << 15) & TEMPERING_MASK_C; // TEMPERING_SHIFT_T(y)
        y ^= (y >>> 18); // TEMPERING_SHIFT_L(y)

        if (mti >= N) // generate N words at one time
        {
          int kk;
          final int[] mt = this.mt; // locals are slightly faster
          final int[] mag01 = this.mag01; // locals are slightly faster

          for (kk = 0; kk < N - M; kk++) {
            z = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
            mt[kk] = mt[kk + M] ^ (z >>> 1) ^ mag01[z & 0x1];
          }
          for (; kk < N - 1; kk++) {
            z = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
            mt[kk] = mt[kk + (M - N)] ^ (z >>> 1) ^ mag01[z & 0x1];
          }
          z = (mt[N - 1] & UPPER_MASK) | (mt[0] & LOWER_MASK);
          mt[N - 1] = mt[M - 1] ^ (z >>> 1) ^ mag01[z & 0x1];

          mti = 0;
        }

        z = mt[mti++];
        z ^= z >>> 11; // TEMPERING_SHIFT_U(z)
        z ^= (z << 7) & TEMPERING_MASK_B; // TEMPERING_SHIFT_S(z)
        z ^= (z << 15) & TEMPERING_MASK_C; // TEMPERING_SHIFT_T(z)
        z ^= (z >>> 18); // TEMPERING_SHIFT_L(z)

        if (mti >= N) // generate N words at one time
        {
          int kk;
          final int[] mt = this.mt; // locals are slightly faster
          final int[] mag01 = this.mag01; // locals are slightly faster

          for (kk = 0; kk < N - M; kk++) {
            a = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
            mt[kk] = mt[kk + M] ^ (a >>> 1) ^ mag01[a & 0x1];
          }
          for (; kk < N - 1; kk++) {
            a = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
            mt[kk] = mt[kk + (M - N)] ^ (a >>> 1) ^ mag01[a & 0x1];
          }
          a = (mt[N - 1] & UPPER_MASK) | (mt[0] & LOWER_MASK);
          mt[N - 1] = mt[M - 1] ^ (a >>> 1) ^ mag01[a & 0x1];

          mti = 0;
        }

        a = mt[mti++];
        a ^= a >>> 11; // TEMPERING_SHIFT_U(a)
        a ^= (a << 7) & TEMPERING_MASK_B; // TEMPERING_SHIFT_S(a)
        a ^= (a << 15) & TEMPERING_MASK_C; // TEMPERING_SHIFT_T(a)
        a ^= (a >>> 18); // TEMPERING_SHIFT_L(a)

        if (mti >= N) // generate N words at one time
        {
          int kk;
          final int[] mt = this.mt; // locals are slightly faster
          final int[] mag01 = this.mag01; // locals are slightly faster

          for (kk = 0; kk < N - M; kk++) {
            b = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
            mt[kk] = mt[kk + M] ^ (b >>> 1) ^ mag01[b & 0x1];
          }
          for (; kk < N - 1; kk++) {
            b = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
            mt[kk] = mt[kk + (M - N)] ^ (b >>> 1) ^ mag01[b & 0x1];
          }
          b = (mt[N - 1] & UPPER_MASK) | (mt[0] & LOWER_MASK);
          mt[N - 1] = mt[M - 1] ^ (b >>> 1) ^ mag01[b & 0x1];

          mti = 0;
        }

        b = mt[mti++];
        b ^= b >>> 11; // TEMPERING_SHIFT_U(b)
        b ^= (b << 7) & TEMPERING_MASK_B; // TEMPERING_SHIFT_S(b)
        b ^= (b << 15) & TEMPERING_MASK_C; // TEMPERING_SHIFT_T(b)
        b ^= (b >>> 18); // TEMPERING_SHIFT_L(b)

        /* derived from nextDouble documentation in jdk 1.2 docs, see top */
        v1 = 2 * (((((long) (y >>> 6)) << 27) + (z >>> 5)) / (double) (1L << 53)) - 1;
        v2 = 2 * (((((long) (a >>> 6)) << 27) + (b >>> 5)) / (double) (1L << 53)) - 1;
        s = v1 * v1 + v2 * v2;
      } while (s >= 1 || s == 0);
      double multiplier = StrictMath.sqrt(-2 * StrictMath.log(s) / s);
      __nextNextGaussian = v2 * multiplier;
      __haveNextNextGaussian = true;
      return v1 * multiplier;
    }
  }

  /**
   * Returns a random float in the half-open range from [0.0f,1.0f). Thus 0.0f is a valid result but 1.0f is not.
   */
  public final float nextFloat() {
    int y;

    if (mti >= N) // generate N words at one time
    {
      int kk;
      final int[] mt = this.mt; // locals are slightly faster
      final int[] mag01 = this.mag01; // locals are slightly faster

      for (kk = 0; kk < N - M; kk++) {
        y = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
        mt[kk] = mt[kk + M] ^ (y >>> 1) ^ mag01[y & 0x1];
      }
      for (; kk < N - 1; kk++) {
        y = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
        mt[kk] = mt[kk + (M - N)] ^ (y >>> 1) ^ mag01[y & 0x1];
      }
      y = (mt[N - 1] & UPPER_MASK) | (mt[0] & LOWER_MASK);
      mt[N - 1] = mt[M - 1] ^ (y >>> 1) ^ mag01[y & 0x1];

      mti = 0;
    }

    y = mt[mti++];
    y ^= y >>> 11; // TEMPERING_SHIFT_U(y)
    y ^= (y << 7) & TEMPERING_MASK_B; // TEMPERING_SHIFT_S(y)
    y ^= (y << 15) & TEMPERING_MASK_C; // TEMPERING_SHIFT_T(y)
    y ^= (y >>> 18); // TEMPERING_SHIFT_L(y)

    return (y >>> 8) / ((float) (1 << 24));
  }

  /**
   * Returns a float in the range from 0.0f to 1.0f, possibly inclusive of 0.0f and 1.0f themselves. Thus:
   * 
   * <p>
   * <table border=0>
   * <th>
   * <td>Expression
   * <td>Interval
   * <tr>
   * <td>nextFloat(false, false)
   * <td>(0.0f, 1.0f)
   * <tr>
   * <td>nextFloat(true, false)
   * <td>[0.0f, 1.0f)
   * <tr>
   * <td>nextFloat(false, true)
   * <td>(0.0f, 1.0f]
   * <tr>
   * <td>nextFloat(true, true)
   * <td>[0.0f, 1.0f]
   * </table>
   * 
   * <p>
   * This version preserves all possible random values in the float range.
   */
  public double nextFloat(boolean includeZero, boolean includeOne) {
    float d = 0.0f;
    do {
      d = nextFloat(); // grab a value, initially from half-open [0.0f, 1.0f)
      if (includeOne && nextBoolean())
        d += 1.0f; // if includeOne, with 1/2 probability, push to [1.0f, 2.0f)
    } while ((d > 1.0f) || // everything above 1.0f is always invalid
        (!includeZero && d == 0.0f)); // if we're not including zero, 0.0f is invalid
    return d;
  }

  /**
   * Returns an integer drawn uniformly from 0 to n-1. Suffice it to say, n must be > 0, or an IllegalArgumentException is raised.
   */
  public final int nextInt(final int n) {
    if (n <= 0)
      throw new IllegalArgumentException("n must be positive, got: " + n);

    if ((n & -n) == n) // i.e., n is a power of 2
    {
      int y;

      if (mti >= N) // generate N words at one time
      {
        int kk;
        final int[] mt = this.mt; // locals are slightly faster
        final int[] mag01 = this.mag01; // locals are slightly faster

        for (kk = 0; kk < N - M; kk++) {
          y = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
          mt[kk] = mt[kk + M] ^ (y >>> 1) ^ mag01[y & 0x1];
        }
        for (; kk < N - 1; kk++) {
          y = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
          mt[kk] = mt[kk + (M - N)] ^ (y >>> 1) ^ mag01[y & 0x1];
        }
        y = (mt[N - 1] & UPPER_MASK) | (mt[0] & LOWER_MASK);
        mt[N - 1] = mt[M - 1] ^ (y >>> 1) ^ mag01[y & 0x1];

        mti = 0;
      }

      y = mt[mti++];
      y ^= y >>> 11; // TEMPERING_SHIFT_U(y)
      y ^= (y << 7) & TEMPERING_MASK_B; // TEMPERING_SHIFT_S(y)
      y ^= (y << 15) & TEMPERING_MASK_C; // TEMPERING_SHIFT_T(y)
      y ^= (y >>> 18); // TEMPERING_SHIFT_L(y)

      return (int) ((n * (long) (y >>> 1)) >> 31);
    }

    int bits, val;
    do {
      int y;

      if (mti >= N) // generate N words at one time
      {
        int kk;
        final int[] mt = this.mt; // locals are slightly faster
        final int[] mag01 = this.mag01; // locals are slightly faster

        for (kk = 0; kk < N - M; kk++) {
          y = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
          mt[kk] = mt[kk + M] ^ (y >>> 1) ^ mag01[y & 0x1];
        }
        for (; kk < N - 1; kk++) {
          y = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
          mt[kk] = mt[kk + (M - N)] ^ (y >>> 1) ^ mag01[y & 0x1];
        }
        y = (mt[N - 1] & UPPER_MASK) | (mt[0] & LOWER_MASK);
        mt[N - 1] = mt[M - 1] ^ (y >>> 1) ^ mag01[y & 0x1];

        mti = 0;
      }

      y = mt[mti++];
      y ^= y >>> 11; // TEMPERING_SHIFT_U(y)
      y ^= (y << 7) & TEMPERING_MASK_B; // TEMPERING_SHIFT_S(y)
      y ^= (y << 15) & TEMPERING_MASK_C; // TEMPERING_SHIFT_T(y)
      y ^= (y >>> 18); // TEMPERING_SHIFT_L(y)

      bits = (y >>> 1);
      val = bits % n;
    } while (bits - val + (n - 1) < 0);
    return val;
  }
}
