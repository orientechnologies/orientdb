/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.core.security;

import com.orientechnologies.common.collection.OLRUCache;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.exception.OSecurityException;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

public class OSecurityManager {
  public static final String HASH_ALGORITHM = "SHA-256";
  public static final String HASH_ALGORITHM_PREFIX = "{" + HASH_ALGORITHM + "}";

  public static final String PBKDF2_ALGORITHM = "PBKDF2WithHmacSHA1";
  public static final String PBKDF2_ALGORITHM_PREFIX = "{" + PBKDF2_ALGORITHM + "}";

  public static final String PBKDF2_SHA256_ALGORITHM = "PBKDF2WithHmacSHA256";
  public static final String PBKDF2_SHA256_ALGORITHM_PREFIX = "{" + PBKDF2_SHA256_ALGORITHM + "}";

  public static final int SALT_SIZE = 24;
  public static final int HASH_SIZE = 24;

  private static final OSecurityManager instance = new OSecurityManager();

  private static Map<String, byte[]> SALT_CACHE = null;

  static {
    final int cacheSize =
        OGlobalConfiguration.SECURITY_USER_PASSWORD_SALT_CACHE_SIZE.getValueAsInteger();
    if (cacheSize > 0) {
      SALT_CACHE = Collections.synchronizedMap(new OLRUCache<String, byte[]>(cacheSize));
    }
  }

  public OSecurityManager() {}

  public static String createHash(final String iInput, String iAlgorithm)
      throws NoSuchAlgorithmException, UnsupportedEncodingException {
    if (iAlgorithm == null) iAlgorithm = HASH_ALGORITHM;

    final MessageDigest msgDigest = MessageDigest.getInstance(iAlgorithm);

    return byteArrayToHexStr(msgDigest.digest(iInput.getBytes("UTF-8")));
  }

  public static OSecurityManager instance() {
    return instance;
  }

  /**
   * Checks if an hash string matches a password, based on the algorithm found on hash string.
   *
   * @param iHash Hash string. Can contain the algorithm as prefix in the format <code>
   *     {ALGORITHM}-HASH</code>.
   * @param iPassword
   * @return
   */
  public static boolean checkPassword(final String iPassword, final String iHash) {
    if (iHash.startsWith(HASH_ALGORITHM_PREFIX)) {
      final String s = iHash.substring(HASH_ALGORITHM_PREFIX.length());
      return createSHA256(iPassword).equals(s);

    } else if (iHash.startsWith(PBKDF2_ALGORITHM_PREFIX)) {
      final String s = iHash.substring(PBKDF2_ALGORITHM_PREFIX.length());
      return checkPasswordWithSalt(iPassword, s, PBKDF2_ALGORITHM);

    } else if (iHash.startsWith(PBKDF2_SHA256_ALGORITHM_PREFIX)) {
      final String s = iHash.substring(PBKDF2_SHA256_ALGORITHM_PREFIX.length());
      return checkPasswordWithSalt(iPassword, s, PBKDF2_SHA256_ALGORITHM);
    }

    // Do not compare raw strings against each other, to avoid timing attacks.
    // Instead, hash them both with a cryptographic hash function and
    // compare their hashes with a constant-time comparison method.
    return MessageDigest.isEqual(digestSHA256(iPassword), digestSHA256(iHash));
  }

  public static String createSHA256(final String iInput) {
    return byteArrayToHexStr(digestSHA256(iInput));
  }

  /**
   * Hashes the input string.
   *
   * @param iInput String to hash
   * @param iIncludeAlgorithm Include the algorithm used or not
   * @return
   */
  public static String createHash(
      final String iInput, final String iAlgorithm, final boolean iIncludeAlgorithm) {
    if (iInput == null) throw new IllegalArgumentException("Input string is null");

    if (iAlgorithm == null) throw new IllegalArgumentException("Algorithm is null");

    final StringBuilder buffer = new StringBuilder(128);

    final String algorithm = validateAlgorithm(iAlgorithm);

    if (iIncludeAlgorithm) {
      buffer.append('{');
      buffer.append(algorithm);
      buffer.append('}');
    }

    final String transformed;
    if (HASH_ALGORITHM.equalsIgnoreCase(algorithm)) {
      transformed = createSHA256(iInput);
    } else if (PBKDF2_ALGORITHM.equalsIgnoreCase(algorithm)) {
      transformed =
          createHashWithSalt(
              iInput,
              OGlobalConfiguration.SECURITY_USER_PASSWORD_SALT_ITERATIONS.getValueAsInteger(),
              algorithm);
    } else if (PBKDF2_SHA256_ALGORITHM.equalsIgnoreCase(algorithm)) {
      transformed =
          createHashWithSalt(
              iInput,
              OGlobalConfiguration.SECURITY_USER_PASSWORD_SALT_ITERATIONS.getValueAsInteger(),
              algorithm);
    } else throw new IllegalArgumentException("Algorithm '" + algorithm + "' is not supported");

    buffer.append(transformed);

    return buffer.toString();
  }

  public static synchronized byte[] digestSHA256(final String iInput) {
    if (iInput == null) return null;

    try {
      MessageDigest md = MessageDigest.getInstance(HASH_ALGORITHM);
      return md.digest(iInput.getBytes("UTF-8"));
    } catch (UnsupportedEncodingException | NoSuchAlgorithmException e) {
      final String message =
          "The requested encoding is not supported: cannot execute security checks";
      OLogManager.instance().error(OSecuritySystem.class, message, e);

      throw OException.wrapException(new OConfigurationException(message), e);
    }
  }

  public static String createHashWithSalt(final String iPassword) {
    return createHashWithSalt(
        iPassword,
        OGlobalConfiguration.SECURITY_USER_PASSWORD_SALT_ITERATIONS.getValueAsInteger(),
        OGlobalConfiguration.SECURITY_USER_PASSWORD_DEFAULT_ALGORITHM.getValueAsString());
  }

  public static String createHashWithSalt(
      final String iPassword, final int iIterations, final String algorithm) {
    final SecureRandom random = new SecureRandom();
    final byte[] salt = new byte[SALT_SIZE];
    random.nextBytes(salt);

    // Hash the password
    final byte[] hash =
        getPbkdf2(iPassword, salt, iIterations, HASH_SIZE, validateAlgorithm(algorithm));

    return byteArrayToHexStr(hash) + ":" + byteArrayToHexStr(salt) + ":" + iIterations;
  }

  public static boolean checkPasswordWithSalt(final String iPassword, final String iHash) {
    return checkPasswordWithSalt(
        iPassword,
        iHash,
        OGlobalConfiguration.SECURITY_USER_PASSWORD_DEFAULT_ALGORITHM.getValueAsString());
  }

  public static boolean checkPasswordWithSalt(
      final String iPassword, final String iHash, final String algorithm) {

    if (!isAlgorithmSupported(algorithm)) {
      OLogManager.instance()
          .error(
              OSecuritySystem.class,
              "The password hash algorithm is not supported: %s",
              null,
              algorithm);
      return false;
    }

    // SPLIT PARTS
    final String[] params = iHash.split(":");
    if (params.length != 3)
      throw new IllegalArgumentException(
          "Hash does not contain the requested parts: <hash>:<salt>:<iterations>");

    final byte[] hash = hexToByteArray(params[0]);
    final byte[] salt = hexToByteArray(params[1]);
    final int iterations = Integer.parseInt(params[2]);

    final byte[] testHash = getPbkdf2(iPassword, salt, iterations, hash.length, algorithm);
    return MessageDigest.isEqual(hash, testHash);
  }

  private static byte[] getPbkdf2(
      final String iPassword,
      final byte[] salt,
      final int iterations,
      final int bytes,
      final String algorithm) {
    String cacheKey = null;

    final String hashedPassword = createSHA256(iPassword + new String(salt));

    if (SALT_CACHE != null) {
      // SEARCH IN CACHE FIRST
      cacheKey = hashedPassword + "|" + Arrays.toString(salt) + "|" + iterations + "|" + bytes;
      final byte[] encoded = SALT_CACHE.get(cacheKey);
      if (encoded != null) return encoded;
    }

    final PBEKeySpec spec = new PBEKeySpec(iPassword.toCharArray(), salt, iterations, bytes * 8);
    final SecretKeyFactory skf;
    try {
      skf = SecretKeyFactory.getInstance(algorithm);
      final byte[] encoded = skf.generateSecret(spec).getEncoded();

      if (SALT_CACHE != null) {
        // SAVE IT IN CACHE
        SALT_CACHE.put(cacheKey, encoded);
      }

      return encoded;
    } catch (Exception e) {
      throw OException.wrapException(
          new OSecurityException("Cannot create a key with '" + algorithm + "' algorithm"), e);
    }
  }

  /** Returns true if the algorithm is supported by the current version of Java */
  private static boolean isAlgorithmSupported(final String algorithm) {
    // Java 7 specific checks.
    if (Runtime.class.getPackage() != null
        && Runtime.class.getPackage().getImplementationVersion() != null) {
      if (Runtime.class.getPackage().getImplementationVersion().startsWith("1.7")) {
        // Java 7 does not support the PBKDF2_SHA256_ALGORITHM.
        if (algorithm != null && algorithm.equals(PBKDF2_SHA256_ALGORITHM)) {
          return false;
        }
      }
    }

    return true;
  }

  private static String validateAlgorithm(final String iAlgorithm) {
    String validAlgo = iAlgorithm;

    if (!isAlgorithmSupported(iAlgorithm)) {
      // Downgrade it to PBKDF2_ALGORITHM.
      validAlgo = PBKDF2_ALGORITHM;

      OLogManager.instance()
          .debug(
              OSecuritySystem.class,
              "The %s algorithm is not supported, downgrading to %s",
              iAlgorithm,
              validAlgo);
    }

    return validAlgo;
  }

  public static String byteArrayToHexStr(final byte[] data) {
    if (data == null) return null;

    final char[] chars = new char[data.length * 2];
    for (int i = 0; i < data.length; i++) {
      final byte current = data[i];
      final int hi = (current & 0xF0) >> 4;
      final int lo = current & 0x0F;
      chars[2 * i] = (char) (hi < 10 ? ('0' + hi) : ('A' + hi - 10));
      chars[2 * i + 1] = (char) (lo < 10 ? ('0' + lo) : ('A' + lo - 10));
    }
    return new String(chars);
  }

  private static byte[] hexToByteArray(final String data) {
    final byte[] hex = new byte[data.length() / 2];
    for (int i = 0; i < hex.length; i++)
      hex[i] = (byte) Integer.parseInt(data.substring(2 * i, 2 * i + 2), 16);

    return hex;
  }

  public OCredentialInterceptor newCredentialInterceptor() {
    OCredentialInterceptor ci = null;

    try {
      String ciClass = OGlobalConfiguration.CLIENT_CREDENTIAL_INTERCEPTOR.getValueAsString();

      if (ciClass != null) {
        Class<?> cls = Class.forName(ciClass); // Throws a ClassNotFoundException if not found.

        if (OCredentialInterceptor.class.isAssignableFrom(cls)) {
          ci = (OCredentialInterceptor) cls.newInstance();
        }
      }
    } catch (Exception ex) {
      OLogManager.instance()
          .debug(this, "newCredentialInterceptor() Exception creating CredentialInterceptor", ex);
    }

    return ci;
  }
}
