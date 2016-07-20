package com.orientechnologies.orient.core.encryption.impl;

import com.orientechnologies.orient.core.encryption.OEncryption;
import com.orientechnologies.orient.core.encryption.OEncryptionFactory;

import java.util.Arrays;
import java.util.Random;

import static org.assertj.core.api.Assertions.*;

/**
 * @author Luca Garulli
 * @since 05.06.13
 */
public abstract class AbstractEncryptionTest {
  public static void testEncryption(String name) {
    testEncryption(name, null);
  }

  public static void testEncryption(String name, String options) {
    long seed = System.currentTimeMillis();
    System.out.println(name + " - Encryption seed " + seed);

    Random random = new Random(seed);
    final int iterationsCount = 1000;
    long encryptedSize = 0;
    for (int i = 0; i < iterationsCount; i++) {
      int contentSize = random.nextInt(10 * 1024 - 100) + 100;
      byte[] content = new byte[contentSize];
      random.nextBytes(content);

      final OEncryption en = OEncryptionFactory.INSTANCE.getEncryption(name, options);

      // FULL
      byte[] encryptedContent = en.encrypt(content);

      encryptedSize += encryptedContent.length;

      assertThat(content).isEqualTo(en.decrypt(encryptedContent));
      // PARTIAL (BUT FULL)
      encryptedContent = en.encrypt(content, 0, content.length);

      encryptedSize += encryptedContent.length;

      assertThat(content).isEqualTo(en.decrypt(encryptedContent));

      // REAL PARTIAL
      encryptedContent = en.encrypt(content, 1, content.length - 2);

      encryptedSize += encryptedContent.length - 2;

      assertThat(Arrays.copyOfRange(content, 1, content.length - 1)).isEqualTo(en.decrypt(encryptedContent));
    }

    System.out.println(
        "Encryption/Decryption test against " + name + " took: " + (System.currentTimeMillis() - seed) + "ms, total byte size: "
            + encryptedSize);
  }
}
