package com.orientechnologies.orient.core.encryption.impl;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.encryption.OEncryption;
import com.orientechnologies.orient.core.exception.OInvalidStorageEncryptionKeyException;
import com.orientechnologies.orient.core.exception.OSecurityException;
import com.orientechnologies.orient.core.serialization.OBase64Utils;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

/***
 * Stateful compression implementation that encrypt the content using AES
 * (https://docs.oracle.com/javase/7/docs/technotes/guides/security/SunProviders.html). Issue
 * https://github.com/orientechnologies/orientdb/issues/89.
 * 
 * @author giastfader
 * @author Luca Garulli
 *
 */
public class OAESEncryption extends OAbstractEncryption {
  // @see https://docs.oracle.com/javase/7/docs/technotes/guides/security/SunProviders.html#SunJCEProvider
  private final String       TRANSFORMATION = "AES/ECB/PKCS5Padding"; // we use ECB because we cannot store the
  private final String       ALGORITHM_NAME = "AES";

  private SecretKeySpec      theKey;
  private Cipher             cipher;

  private boolean            initialized    = false;

  public static final String NAME           = "aes";

  @Override
  public String name() {
    return NAME;
  }

  public OAESEncryption() {
  }

  public OEncryption configure(final String iOptions) {
    initialized = false;

    if (iOptions == null)
      throw new OSecurityException(
          "AES encryption has been selected, but no key was found. Please configure it by passing the key as property at database create/open. The property key is: '"
              + OGlobalConfiguration.STORAGE_ENCRYPTION_KEY.getKey() + "'");

    try {
      final byte[] key = OBase64Utils.decode(iOptions);

      theKey = new SecretKeySpec(key, ALGORITHM_NAME); // AES
      cipher = Cipher.getInstance(TRANSFORMATION);

    } catch (Exception e) {
      throw OException.wrapException(new OInvalidStorageEncryptionKeyException(
          "Cannot initialize AES encryption with current key. Assure the key is a BASE64 - 128 oe 256 bits long"), e);

    }

    this.initialized = true;

    return this;
  }

  public byte[] encryptOrDecrypt(final int mode, final byte[] input, final int offset, final int length) throws Exception {
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
