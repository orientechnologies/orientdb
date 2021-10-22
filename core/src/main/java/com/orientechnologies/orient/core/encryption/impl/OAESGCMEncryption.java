package com.orientechnologies.orient.core.encryption.impl;

import static java.lang.String.format;
import static javax.crypto.Cipher.DECRYPT_MODE;
import static javax.crypto.Cipher.ENCRYPT_MODE;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.thread.NonDaemonThreadFactory;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.encryption.OEncryption;
import com.orientechnologies.orient.core.exception.OInvalidStorageEncryptionKeyException;
import com.orientechnologies.orient.core.exception.OSecurityException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Base64;
import java.util.concurrent.*;
import javax.crypto.AEADBadTagException;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.ShortBufferException;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

/**
 * OEncryption implementation using AES/GCM/NoPadding with a 12 byte nonce and 16 byte tag size.
 *
 * @author Skymatic / Markus Kreusch (markus.kreusch--(at)--skymatic.de)
 * @author Skymatic / Sebastian Stenzel (sebastian.stenzel--(at)--skymatic.de)
 */
public class OAESGCMEncryption implements OEncryption {

  public static final String NAME = "aes/gcm";

  private static final String ALGORITHM_NAME = "AES";
  private static final String TRANSFORMATION = "AES/GCM/NoPadding";
  // Cipher.getInstance is slow, so we don't want to call it in every encrypt/decrypt call. Instead
  // we reuse existing instances:
  private static final ThreadLocal<Cipher> CIPHER =
      ThreadLocal.withInitial(OAESGCMEncryption::getCipherInstance);

  private static final int GCM_NONCE_SIZE_IN_BYTES = 12;
  private static final int GCM_TAG_SIZE_IN_BYTES = 16;
  private static final int MIN_CIPHERTEXT_SIZE = GCM_NONCE_SIZE_IN_BYTES + GCM_TAG_SIZE_IN_BYTES;

  private static final String NO_SUCH_CIPHER = "AES/GCM/NoPadding not supported.";
  private static final String MISSING_KEY_ERROR =
      "AESGCMEncryption encryption has been selected, "
          + "but no key was found. Please configure it by passing the key as property at database create/open. The property key is: '%s'";
  private static final String INVALID_KEY_ERROR =
      "Failed to initialize AESGCMEncryption. Assure the key is a 128, 192 or 256 bits long BASE64 value";
  private static final String ENCRYPTION_NOT_INITIALIZED_ERROR =
      "OAESGCMEncryption not properly initialized";
  private static final String AUTHENTICATION_ERROR =
      "Authentication of encrypted data failed. The encrypted data may have been altered or the used key is incorrect";
  private static final String INVALID_CIPHERTEXT_SIZE_ERROR =
      "Invalid ciphertext size: minimum: %d, actual: %d";
  private static final String INVALID_RANGE_ERROR =
      "Invalid range: array size: %d, offset: %d, length: %d";
  private static final String BLOCKING_SECURE_RANDOM_ERROR =
      "SecureRandom blocked while retrieving randomness. This maybe caused by a misconfigured or absent random source on your operating system.";

  private boolean initialized;
  private SecretKey key;
  private SecureRandom csprng;

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public OEncryption configure(final String base64EncodedKey) {
    initialized = false;

    key = createKey(base64EncodedKey);
    csprng = createSecureRandom();
    getCipherInstance(); // fail early if cipher algorithm is not available

    initialized = true;
    return this;
  }

  @Override
  public byte[] encrypt(byte[] input) {
    return encrypt(input, 0, input.length);
  }

  @Override
  public byte[] decrypt(byte[] input) {
    return decrypt(input, 0, input.length);
  }

  @Override
  public byte[] encrypt(final byte[] input, final int offset, final int length) {
    assertInitialized();
    assertRangeIsValid(input.length, offset, length);

    byte[] nonce = randomNonce();
    Cipher cipher = getAndInitializeCipher(ENCRYPT_MODE, nonce);

    int outputLength = GCM_NONCE_SIZE_IN_BYTES + cipher.getOutputSize(length);
    byte[] output = Arrays.copyOf(nonce, outputLength);

    try {
      cipher.doFinal(input, offset, length, output, GCM_NONCE_SIZE_IN_BYTES);
      return output;
    } catch (IllegalBlockSizeException | BadPaddingException | ShortBufferException e) {
      throw new IllegalStateException("Unexpected exception during GCM decryption.", e);
    }
  }

  @Override
  public byte[] decrypt(final byte[] input, final int offset, final int length) {
    assertInitialized();
    assertRangeIsValid(input.length, offset, length);
    assertCiphertextSizeIsValid(length);

    byte[] nonce = readNonce(input);
    Cipher cipher = getAndInitializeCipher(DECRYPT_MODE, nonce);

    try {
      return cipher.doFinal(
          input, offset + GCM_NONCE_SIZE_IN_BYTES, length - GCM_NONCE_SIZE_IN_BYTES);
    } catch (AEADBadTagException e) {
      throw OException.wrapException(new OSecurityException(AUTHENTICATION_ERROR), e);
    } catch (IllegalBlockSizeException | BadPaddingException e) {
      throw new IllegalStateException("Unexpected exception during GCM decryption.", e);
    }
  }

  private SecretKey createKey(String base64EncodedKey) {
    if (base64EncodedKey == null) {
      throw new OSecurityException(
          format(MISSING_KEY_ERROR, OGlobalConfiguration.STORAGE_ENCRYPTION_KEY.getKey()));
    }
    try {
      final byte[] keyBytes = Base64.getDecoder().decode(base64EncodedKey.getBytes());
      validateKeySize(keyBytes.length);
      return new SecretKeySpec(keyBytes, ALGORITHM_NAME);
    } catch (IllegalArgumentException e) {
      throw OException.wrapException(
          new OInvalidStorageEncryptionKeyException(INVALID_KEY_ERROR), e);
    }
  }

  private SecureRandom createSecureRandom() {
    SecureRandom secureRandom = new SecureRandom();
    assertNonBlocking(secureRandom);
    return secureRandom;
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

  private void assertRangeIsValid(int arraySize, int offset, int length) {
    if (offset >= arraySize || offset + length > arraySize) {
      throw new IllegalArgumentException(format(INVALID_RANGE_ERROR, arraySize, offset, length));
    }
  }

  private void assertCiphertextSizeIsValid(int size) {
    if (size < MIN_CIPHERTEXT_SIZE) {
      throw new OSecurityException(
          format(INVALID_CIPHERTEXT_SIZE_ERROR, MIN_CIPHERTEXT_SIZE, size));
    }
  }

  private byte[] randomNonce() {
    byte[] nonce = new byte[GCM_NONCE_SIZE_IN_BYTES];
    csprng.nextBytes(nonce);
    return nonce;
  }

  private byte[] readNonce(byte[] input) {
    return Arrays.copyOf(input, GCM_NONCE_SIZE_IN_BYTES);
  }

  private Cipher getAndInitializeCipher(final int mode, final byte[] nonce) {
    try {
      Cipher cipher = CIPHER.get();
      cipher.init(mode, key, gcmParameterSpec(nonce));
      return cipher;
    } catch (InvalidKeyException e) {
      throw OException.wrapException(new OInvalidStorageEncryptionKeyException(e.getMessage()), e);
    } catch (InvalidAlgorithmParameterException e) {
      throw new IllegalArgumentException("Invalid or re-used nonce.", e);
    }
  }

  private void assertNonBlocking(SecureRandom secureRandom) {
    ExecutorService executor =
        Executors.newSingleThreadExecutor(new NonDaemonThreadFactory("OAESGCMEncryption thread"));
    try {
      executor.submit(() -> secureRandom.nextInt()).get(1, TimeUnit.MINUTES);
    } catch (InterruptedException | ExecutionException e) {
      throw new IllegalStateException(e);
    } catch (TimeoutException e) {
      throw new OSecurityException(BLOCKING_SECURE_RANDOM_ERROR);
    } finally {
      executor.shutdownNow();
    }
  }

  private GCMParameterSpec gcmParameterSpec(byte[] nonce) {
    return new GCMParameterSpec(GCM_TAG_SIZE_IN_BYTES * Byte.SIZE, nonce);
  }

  private static Cipher getCipherInstance() {
    try {
      return Cipher.getInstance(TRANSFORMATION);
    } catch (NoSuchAlgorithmException | NoSuchPaddingException e) {
      throw OException.wrapException(new OSecurityException(NO_SUCH_CIPHER), e);
    }
  }
}
