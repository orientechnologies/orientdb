package com.orientechnologies.orient.core.encryption.impl;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.encryption.OEncryption;
import com.orientechnologies.orient.core.exception.OInvalidStorageEncryptionKeyException;
import com.orientechnologies.orient.core.exception.OSecurityException;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.DESKeySpec;
import java.util.Base64;

/***
 * Stateful compression implementation that encrypt the content using DES algorithm
 * (https://docs.oracle.com/javase/7/docs/technotes/guides/security/SunProviders.html). Issue
 * https://github.com/orientechnologies/orientdb/issues/89.
 * 
 * @author giastfader
 *
 */
public class ODESEncryption extends OAbstractEncryption {
  // @see https://docs.oracle.com/javase/7/docs/technotes/guides/security/SunProviders.html#SunJCEProvider
  private final String       TRANSFORMATION = "DES/ECB/PKCS5Padding"; // //we use ECB because we cannot
  private final String       ALGORITHM_NAME = "DES";

  private SecretKey          theKey;
  private Cipher             cipher;

  private boolean            initialized    = false;

  public static final String NAME           = "des";

  @Override
  public String name() {
    return NAME;
  }

  public ODESEncryption() {
  }

  public OEncryption configure(final String iOptions) {
    initialized = false;

    if (iOptions == null)
      throw new OSecurityException(
          "DES encryption has been selected, but no key was found. Please configure it by passing the key as property at database create/open. The property key is: '"
              + OGlobalConfiguration.STORAGE_ENCRYPTION_KEY.getKey() + "'");

    try {
      final byte[] key =  Base64.getDecoder().decode(iOptions);

      final DESKeySpec desKeySpec = new DESKeySpec(key);
      final SecretKeyFactory keyFactory = SecretKeyFactory.getInstance(ALGORITHM_NAME);

      theKey = keyFactory.generateSecret(desKeySpec);
      cipher = Cipher.getInstance(TRANSFORMATION);

    } catch (Exception e) {
      throw OException.wrapException(new OInvalidStorageEncryptionKeyException(
          "Cannot initialize DES encryption with current key. Assure the key is a BASE64 - 64 bits long"), e);
    }

    this.initialized = true;

    return this;
  }

  public byte[] encryptOrDecrypt(final int mode, final byte[] input, final int offset, final int length) throws Exception {
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
