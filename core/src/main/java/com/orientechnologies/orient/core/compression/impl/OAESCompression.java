package com.orientechnologies.orient.core.compression.impl;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.OSecurityException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.serialization.OBase64Utils;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

/***
 * Compression implementation that encrypt the content using AES
 * (https://docs.oracle.com/javase/7/docs/technotes/guides/security/SunProviders.html). Issue
 * https://github.com/orientechnologies/orientdb/issues/89.
 * 
 * @author giastfader
 *
 */
public class OAESCompression extends OAbstractEncryptedCompression {
  // @see https://docs.oracle.com/javase/7/docs/technotes/guides/security/SunProviders.html#SunJCEProvider
  private final String        TRANSFORMATION = "AES/ECB/PKCS5Padding"; // we use ECB because we cannot store the
  private final String        ALGORITHM_NAME = "AES";

  private final SecretKeySpec theKey;
  private final Cipher        cipher;

  private boolean             initialized    = false;

  public static final String  NAME           = "aes-encrypted";

  @Override
  public String name() {
    return NAME;
  }

  public OAESCompression() {
    initialized = false;

    final String configuredKey = OGlobalConfiguration.STORAGE_ENCRYPTION_AES_KEY.getValueAsString();

    if (configuredKey == null)
      throw new OStorageException("AES compression has been selected, but no key was found. Please configure '"
          + OGlobalConfiguration.STORAGE_ENCRYPTION_AES_KEY.getKey() + "' setting or remove AES compression by setting '"
          + OGlobalConfiguration.STORAGE_COMPRESSION_METHOD.getKey() + "=nothing'");

    try {
      final byte[] key = OBase64Utils.decode(configuredKey);

      theKey = new SecretKeySpec(key, ALGORITHM_NAME); // AES
      cipher = Cipher.getInstance(TRANSFORMATION);

    } catch (Exception e) {
      throw new OSecurityException(
          "Cannot initialize AES encryption with current key. Assure the key is a BASE64 - 128 oe 256 bits long", e);

    }

    this.initialized = true;
  }

  public byte[] encryptOrDecrypt(final int mode, final byte[] input, final int offset, final int length) throws Throwable {
    if (!initialized)
      throw new OSecurityException("AES encryption algorithm is not available");

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
