package com.orientechnologies.orient.core.encryption.impl;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.encryption.OEncryption;
import com.orientechnologies.orient.core.exception.OInvalidStorageEncryptionKeyException;
import com.orientechnologies.orient.core.exception.OSecurityException;

import javax.crypto.*;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.security.*;
import java.util.Base64;

import static java.lang.String.format;
import static javax.crypto.Cipher.DECRYPT_MODE;
import static javax.crypto.Cipher.ENCRYPT_MODE;

/**
 * OEncryption implementation using AES/GCM/NoPadding with a 12 byte nonce and 16 byte tag size.
 *
 * @author Skymatic / Markus Kreusch (markus.kreusch--(at)--skymatic.de)
 * @author Skymatic / Sebastian Stenzel (sebastian.stenzel--(at)--skymatic.de)
 */
public class OAESGCMEncryption extends OAbstractEncryption {

  public static final String NAME = "aes/gcm";

  private static final String ALGORITHM_NAME = "AES";
  private static final String TRANSFORMATION = "AES/GCM/NoPadding";

  private static final int GCM_NONCE_SIZE_IN_BYTES = 12;
  private static final int GCM_TAG_SIZE_IN_BYTES = 16;

  private static final String MISSING_KEY_ERROR = "AESGCMEncryption encryption has been selected, but no key was found. Please configure it by passing the key as property at database create/open. The property key is: '%s'";
  private static final String INVALID_KEY_ERROR = "Failed to initialize AESGCMEncryption. Assure the key is a 128, 192 or 256 bits long BASE64 value";
  private static final String ENCRYPTION_NOT_INITIALIZED_ERROR = "OAESGCMEncryption not properly initialized";
  private static final String SECURE_RANDOM_CREATION_ERROR = "OAESGCMEncryption could not create SecureRandom";
  private static final String AUTHENTICATION_ERROR = "Authentication of encrypted data failed. The encrypted data may have been altered.";


  private boolean initialized;
  private SecretKey key;
  private SecureRandom csprng;

  @Override
  public String name() {
    return NAME;
  }

  public OAESGCMEncryption() {
  }

  public OEncryption configure(final String base64EncodedKey) {
    initialized = false;

    key = createKey(base64EncodedKey);
    csprng = createSecureRandom();

    initialized = true;
    return this;
  }

  public byte[] encryptOrDecrypt(final int mode, final byte[] input, final int offset, final int length) throws Exception {
    assertInitialized();
    return encryptOrDecrypt(mode, ByteBuffer.wrap(input, offset, length)).array();
  }

  private SecretKey createKey(String base64EncodedKey) {
    if (base64EncodedKey == null) {
      throw new OSecurityException(format(MISSING_KEY_ERROR, OGlobalConfiguration.STORAGE_ENCRYPTION_KEY.getKey()));
    }
    try {
      final byte[] keyBytes = Base64.getDecoder().decode(base64EncodedKey);
      validateKeySize(keyBytes.length);
      return new SecretKeySpec(keyBytes, ALGORITHM_NAME);
    } catch (Exception e) {
      throw OException.wrapException(new OInvalidStorageEncryptionKeyException(INVALID_KEY_ERROR), e);
    }
  }

  private SecureRandom createSecureRandom() {
    try {
      return SecureRandom.getInstanceStrong();
    } catch (NoSuchAlgorithmException e) {
      throw OException.wrapException(new OSecurityException(SECURE_RANDOM_CREATION_ERROR), e);
    }
  }

  private ByteBuffer encryptOrDecrypt(int mode, ByteBuffer input) throws GeneralSecurityException {
    switch (mode) {
      case ENCRYPT_MODE:
        return encrypt(input);
      case DECRYPT_MODE:
        return decrypt(input);
      default:
        throw new IllegalArgumentException(format("Unexpected mode %d", mode));
    }
  }

  private ByteBuffer encrypt(ByteBuffer input) throws GeneralSecurityException {
    Cipher cipher = Cipher.getInstance(TRANSFORMATION);
    byte[] nonce = randomNonce();
    cipher.init(ENCRYPT_MODE, key, gcmParameterSpec(nonce));

    ByteBuffer output = ByteBuffer.allocate(GCM_NONCE_SIZE_IN_BYTES + cipher.getOutputSize(input.remaining()));
    output.put(nonce);
    cipher.doFinal(input, output);
    output.flip();

    return output;
  }

  private ByteBuffer decrypt(ByteBuffer input) throws GeneralSecurityException {
    Cipher cipher = Cipher.getInstance(TRANSFORMATION);
    byte[] nonce = readNonce(input);
    cipher.init(DECRYPT_MODE, key, gcmParameterSpec(nonce));

    ByteBuffer output = ByteBuffer.allocate(cipher.getOutputSize(input.remaining()));
    try {
      cipher.doFinal(input, output);
    } catch (AEADBadTagException e) {
      throw OException.wrapException(new OSecurityException(AUTHENTICATION_ERROR), e);
    }
    output.flip();

    return output;
  }

  private void validateKeySize(int numBytes) {
    if (numBytes != 16 && numBytes != 24 && numBytes != 32) {
      throw new OInvalidStorageEncryptionKeyException(INVALID_KEY_ERROR);
    }
  }

  private void assertInitialized() {
    if (!initialized) {
      throw new OSecurityException(ENCRYPTION_NOT_INITIALIZED_ERROR);
    }
  }

  private byte[] randomNonce() {
    byte[] nonce = new byte[GCM_NONCE_SIZE_IN_BYTES];
    csprng.nextBytes(nonce);
    return nonce;
  }

  private byte[] readNonce(ByteBuffer input) {
    byte[] nonce = new byte[GCM_NONCE_SIZE_IN_BYTES];
    input.get(nonce);
    return nonce;
  }

  private GCMParameterSpec gcmParameterSpec(byte[] nonce) {
    return new GCMParameterSpec(GCM_TAG_SIZE_IN_BYTES * Byte.SIZE, nonce);
  }

}
