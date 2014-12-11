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

import org.testng.annotations.Test;

/**
 * @author Andrey Lomakin
 * @since 19.09.12
 */

public class MersenneTwisterTest {
  @Test
  public void test() {
    int j;

    MersenneTwister r;

    // CORRECTNESS TEST
    // COMPARE WITH http://www.math.keio.ac.jp/matumoto/CODES/MT2002/mt19937ar.out

    r = new MersenneTwister(new int[] { 0x123, 0x234, 0x345, 0x456 });
    System.out.println("Output of MersenneTwister with new (2002/1/26) seeding mechanism");
    for (j = 0; j < 1000; j++) {
      // first, convert the int from signed to "unsigned"
      long l = (long) r.nextInt();
      if (l < 0)
        l += 4294967296L; // max int value
      String s = String.valueOf(l);
      while (s.length() < 10)
        s = " " + s; // buffer
      System.out.print(s + " ");
      if (j % 5 == 4)
        System.out.println();
    }

    // SPEED TEST

    final long SEED = 4357;

    int xx;
    long ms;
    System.out.println("\nTime to test grabbing 100000000 ints");

    r = new MersenneTwister(SEED);
    ms = System.currentTimeMillis();
    xx = 0;
    for (j = 0; j < 100000000; j++)
      xx += r.nextInt();
    System.out.println("Mersenne Twister: " + (System.currentTimeMillis() - ms) + "          Ignore this: " + xx);

    System.out.println("To compare this with java.util.Random, run this same test on MersenneTwisterFast.");
    System.out.println("The comparison with Random is removed from MersenneTwister because it is a proper");
    System.out.println("subclass of Random and this unfairly makes some of Random's methods un-inlinable,");
    System.out.println("so it would make Random look worse than it is.");

    // TEST TO COMPARE TYPE CONVERSION BETWEEN
    // MersenneTwisterFast.java AND MersenneTwister.java

    System.out.println("\nGrab the first 1000 booleans");
    r = new MersenneTwister(SEED);
    for (j = 0; j < 1000; j++) {
      System.out.print(r.nextBoolean() + " ");
      if (j % 8 == 7)
        System.out.println();
    }
    if (!(j % 8 == 7))
      System.out.println();

    System.out.println("\nGrab 1000 booleans of increasing probability using nextBoolean(double)");
    r = new MersenneTwister(SEED);
    for (j = 0; j < 1000; j++) {
      System.out.print(r.nextBoolean((double) (j / 999.0)) + " ");
      if (j % 8 == 7)
        System.out.println();
    }
    if (!(j % 8 == 7))
      System.out.println();

    System.out.println("\nGrab 1000 booleans of increasing probability using nextBoolean(float)");
    r = new MersenneTwister(SEED);
    for (j = 0; j < 1000; j++) {
      System.out.print(r.nextBoolean((float) (j / 999.0f)) + " ");
      if (j % 8 == 7)
        System.out.println();
    }
    if (!(j % 8 == 7))
      System.out.println();

    byte[] bytes = new byte[1000];
    System.out.println("\nGrab the first 1000 bytes using nextBytes");
    r = new MersenneTwister(SEED);
    r.nextBytes(bytes);
    for (j = 0; j < 1000; j++) {
      System.out.print(bytes[j] + " ");
      if (j % 16 == 15)
        System.out.println();
    }
    if (!(j % 16 == 15))
      System.out.println();

    byte b;
    System.out.println("\nGrab the first 1000 bytes -- must be same as nextBytes");
    r = new MersenneTwister(SEED);
    for (j = 0; j < 1000; j++) {
      System.out.print((b = r.nextByte()) + " ");
      if (b != bytes[j])
        System.out.print("BAD ");
      if (j % 16 == 15)
        System.out.println();
    }
    if (!(j % 16 == 15))
      System.out.println();

    System.out.println("\nGrab the first 1000 shorts");
    r = new MersenneTwister(SEED);
    for (j = 0; j < 1000; j++) {
      System.out.print(r.nextShort() + " ");
      if (j % 8 == 7)
        System.out.println();
    }
    if (!(j % 8 == 7))
      System.out.println();

    System.out.println("\nGrab the first 1000 ints");
    r = new MersenneTwister(SEED);
    for (j = 0; j < 1000; j++) {
      System.out.print(r.nextInt() + " ");
      if (j % 4 == 3)
        System.out.println();
    }
    if (!(j % 4 == 3))
      System.out.println();

    System.out.println("\nGrab the first 1000 ints of different sizes");
    r = new MersenneTwister(SEED);
    int max = 1;
    for (j = 0; j < 1000; j++) {
      System.out.print(r.nextInt(max) + " ");
      max *= 2;
      if (max <= 0)
        max = 1;
      if (j % 4 == 3)
        System.out.println();
    }
    if (!(j % 4 == 3))
      System.out.println();

    System.out.println("\nGrab the first 1000 longs");
    r = new MersenneTwister(SEED);
    for (j = 0; j < 1000; j++) {
      System.out.print(r.nextLong() + " ");
      if (j % 3 == 2)
        System.out.println();
    }
    if (!(j % 3 == 2))
      System.out.println();

    System.out.println("\nGrab the first 1000 longs of different sizes");
    r = new MersenneTwister(SEED);
    long max2 = 1;
    for (j = 0; j < 1000; j++) {
      System.out.print(r.nextLong(max2) + " ");
      max2 *= 2;
      if (max2 <= 0)
        max2 = 1;
      if (j % 4 == 3)
        System.out.println();
    }
    if (!(j % 4 == 3))
      System.out.println();

    System.out.println("\nGrab the first 1000 floats");
    r = new MersenneTwister(SEED);
    for (j = 0; j < 1000; j++) {
      System.out.print(r.nextFloat() + " ");
      if (j % 4 == 3)
        System.out.println();
    }
    if (!(j % 4 == 3))
      System.out.println();

    System.out.println("\nGrab the first 1000 doubles");
    r = new MersenneTwister(SEED);
    for (j = 0; j < 1000; j++) {
      System.out.print(r.nextDouble() + " ");
      if (j % 3 == 2)
        System.out.println();
    }
    if (!(j % 3 == 2))
      System.out.println();

    System.out.println("\nGrab the first 1000 gaussian doubles");
    r = new MersenneTwister(SEED);
    for (j = 0; j < 1000; j++) {
      System.out.print(r.nextGaussian() + " ");
      if (j % 3 == 2)
        System.out.println();
    }
    if (!(j % 3 == 2))
      System.out.println();

  }
}
