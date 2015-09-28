package com.orientechnologies.orient.core.compression.impl;

import java.util.Random;

import org.testng.Assert;

import com.orientechnologies.orient.core.compression.OCompression;
import com.orientechnologies.orient.core.compression.OCompressionFactory;

/**
 * @author Andrey Lomakin
 * @since 05.06.13
 */
public abstract class AbstractCompressionTest {
  public static void testCompression(String name) {
    testCompression(name, null);
  }

  public static void testCompression(String name, String options) {
    long seed = System.currentTimeMillis();
    System.out.println(name + " - Compression seed " + seed);

    Random random = new Random(seed);
    final int iterationsCount = 1000;
    long compressedSize = 0;
    for (int i = 0; i < iterationsCount; i++) {
      int contentSize = random.nextInt(10 * 1024 - 100) + 100;
      byte[] content = new byte[contentSize];
      random.nextBytes(content);

      final OCompression compression = OCompressionFactory.INSTANCE.getCompression(name, options);

      final byte[] compressedContent = compression.compress(content);

      compressedSize += compressedContent.length;

      Assert.assertEquals(content, compression.uncompress(compressedContent));
    }

    System.out.println("Compression/Decompression test against " + name + " took: " + (System.currentTimeMillis() - seed)
        + "ms, total byte size: " + compressedSize);
  }
}
