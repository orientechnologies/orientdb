package com.orientechnologies.orient.core.compression.impl;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.OSecurityException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.serialization.OBase64Utils;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.DESKeySpec;

/***
 * Compression implementation that encrypt the content using DES algorithm
 * (https://docs.oracle.com/javase/7/docs/technotes/guides/security/SunProviders.html). Issue
 * https://github.com/orientechnologies/orientdb/issues/89.
 * 
 * @author giastfader
 *
 */
public class ODESCompression extends OAbstractEncryptedCompression {
  // @see https://docs.oracle.com/javase/7/docs/technotes/guides/security/SunProviders.html#SunJCEProvider
  private final String       TRANSFORMATION = "DES/ECB/PKCS5Padding"; // //we use ECB because we cannot
  private final String       ALGORITHM_NAME = "DES";

  private final SecretKey    theKey;
  private final Cipher       cipher;

  private boolean            initialized    = false;

  public static final String NAME           = "des-encrypted";

  @Override
  public String name() {
    return NAME;
  }

  public ODESCompression() {
    initialized = false;

    final String configuredKey = OGlobalConfiguration.STORAGE_ENCRYPTION_DES_KEY.getValueAsString();

    if (configuredKey == null)
      throw new OStorageException("DES compression has been selected, but no key was found. Please configure '"
          + OGlobalConfiguration.STORAGE_ENCRYPTION_DES_KEY.getKey() + "' setting or remove DES compression by setting '"
          + OGlobalConfiguration.STORAGE_COMPRESSION_METHOD.getKey() + "=nothing'");

    try {
      final byte[] key = OBase64Utils.decode(configuredKey);

      final DESKeySpec desKeySpec = new DESKeySpec(key);
      final SecretKeyFactory keyFactory = SecretKeyFactory.getInstance(ALGORITHM_NAME);

      theKey = keyFactory.generateSecret(desKeySpec);
      cipher = Cipher.getInstance(TRANSFORMATION);

    } catch (Exception e) {
      throw new OSecurityException("Cannot initialize DES encryption with current key. Assure the key is a BASE64 - 64 bits long",
          e);
    }

    this.initialized = true;
  }

  public byte[] encryptOrDecrypt(final int mode, final byte[] input, final int offset, final int length) throws Throwable {
    if (!initialized)
      throw new OSecurityException("DES encryption algorithm is not available");

    cipher.init(mode, theKey);

    final byte[] content;
    if (offset == 0 && length == input.length) {
      content = input;
    } else {
      content = new byte[length];
      System.arraycopy(input, offset, content, 0, length);
    }
    return cipher.doFinal(content);
  }
}
