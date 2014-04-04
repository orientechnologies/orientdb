package com.orientechnologies.orient.core.compression.impl;

import com.orientechnologies.orient.core.compression.OCompression;
import com.orientechnologies.orient.core.compression.OCompressionFactory;
import org.testng.Assert;

import java.util.Random;

/**
 * @author Andrey Lomakin
 * @since 05.06.13
 */
public class CompressionTest {
  public static void main(String[] args) {
    // GENERATE CONTENT
    long seed = System.currentTimeMillis();
    Random random = new Random(seed);
    final int iterationsCount = 1000;
    byte[][] contents = new byte[iterationsCount][];

    long compressedSize = 0;

    for (int i = 0; i < iterationsCount; i++) {
      int contentSize = random.nextInt(10 * 1024 - 100) + 100;
      byte[] content = new byte[contentSize];
      random.nextBytes(content);

      compressedSize += contentSize;

      contents[i] = content;
    }

    System.out.println("Starting benchmark against " + iterationsCount + " iterations where total of buffer size = "
        + compressedSize);

    for (String name : OCompressionFactory.INSTANCE.getCompressions()) {
      compressedSize = 0;
      long startTime = System.currentTimeMillis();
      for (int i = 0; i < iterationsCount; i++) {
        final OCompression compression = OCompressionFactory.INSTANCE.getCompression(name);

        final byte[] compressedContent = compression.compress(contents[i]);

        compressedSize += compressedContent.length;

        Assert.assertEquals(contents[i], compression.uncompress(compressedContent));
      }

      System.out.println("Compression/Decompression test against " + name + " took: " + (System.currentTimeMillis() - seed)
          + "ms, total byte size: " + compressedSize);
    }
  }
}
