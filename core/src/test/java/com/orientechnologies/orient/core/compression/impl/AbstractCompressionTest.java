package com.orientechnologies.orient.core.compression.impl;

import com.orientechnologies.orient.core.compression.OCompression;
import com.orientechnologies.orient.core.compression.OCompressionFactory;
import org.testng.Assert;

import java.util.Random;

/**
 * @author Andrey Lomakin
 * @since 05.06.13
 */
public abstract class AbstractCompressionTest {
  public static void testCompression(String name) {
    long seed = System.currentTimeMillis();
    System.out.println(name + " - Compression seed " + seed);

    Random random = new Random(seed);
    final int iterationsCount = 1000;
    long compressedSize = 0;
    for (int i = 0; i < iterationsCount; i++) {
      int contentSize = random.nextInt(10 * 1024 - 100) + 100;
      byte[] content = new byte[contentSize];
      random.nextBytes(content);

      OCompression compression = OCompressionFactory.INSTANCE.getCompression(name);

      final byte[] compressedContent = compression.compress(content);

      compressedSize += compressedContent.length;

      Assert.assertEquals(content, compression.uncompress(compressedContent));
    }

    System.out.println("Compression/Decompression test against " + name + " took: " + (System.currentTimeMillis() - seed)
        + "ms, total byte size: " + compressedSize);
  }
}
