package com.orientechnologies.orient.core.serialization.compression.impl;

import java.util.Random;

import org.testng.Assert;

import com.orientechnologies.orient.core.serialization.compression.OCompression;
import com.orientechnologies.orient.core.serialization.compression.OCompressionFactory;

/**
 * @author Andrey Lomakin
 * @since 05.06.13
 */
public abstract class AbstractCompressionTest {
  protected void testCompression(String name) {
    long seed = System.currentTimeMillis();
    System.out.println("Compression seed " + seed);

    Random random = new Random(seed);
    final int iterationsCount = 1000;
    for (int i = 0; i < iterationsCount; i++) {
      int contentSize = random.nextInt(10 * 1024 - 100) + 100;
      byte[] content = new byte[contentSize];
      random.nextBytes(content);

      OCompression compression = OCompressionFactory.INSTANCE.getCompression(name);

      byte[] compressedContent = compression.compress(content);
      Assert.assertEquals(content, compression.uncompress(compressedContent));
    }
  }
}
