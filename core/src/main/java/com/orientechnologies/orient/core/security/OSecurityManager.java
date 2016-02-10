/*
  *
  *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
  *  * For more information: http://www.orientechnologies.com
  *
  */
package com.orientechnologies.orient.core.security;

import java.io.UnsupportedEncodingException;
import java.security.Key;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.exception.OSecurityAccessException;
import com.orientechnologies.orient.core.exception.OSecurityException;

public class OSecurityManager {

  public static final String ALGORITHM        = "SHA-256";
  public static final String ALGORITHM_PREFIX = "{" + ALGORITHM + "}";

  private static final OSecurityManager instance = new OSecurityManager();

  private MessageDigest md;

  public OSecurityManager() {
    try {
      md = MessageDigest.getInstance(ALGORITHM);
    } catch (NoSuchAlgorithmException e) {
      OLogManager.instance().error(this, "Cannot use OSecurityManager", e);
    }
  }

  public static String digest2String(final String iInput, String iAlgorithm)
      throws NoSuchAlgorithmException, UnsupportedEncodingException {
    if (iAlgorithm == null)
      iAlgorithm = ALGORITHM;

    final MessageDigest msgDigest = MessageDigest.getInstance(iAlgorithm);

    return byteArrayToHexStr(msgDigest.digest(iInput.getBytes("UTF-8")));
  }

  public static OSecurityManager instance() {
    return instance;
  }

  public static String byteArrayToHexStr(final byte[] data) {
    if (data == null)
      return null;

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

  public boolean check(final byte[] iInput1, final byte[] iInput2) {
    return MessageDigest.isEqual(iInput1, iInput2);
  }

  public boolean check(final String iInput1, final byte[] iInput2) {
    return MessageDigest.isEqual(digest(iInput1), iInput2);
  }

  public boolean check(String iInput1, String iInput2) {
    if (iInput2.startsWith(ALGORITHM_PREFIX)) {
      // iInput2 is a representation of the expected hash of iInput1

      // create the equivalent string representation of the hashed input 1
      iInput1 = digest2String(iInput1);

      // strip the algorithm prefix, getting the expected hash
      iInput2 = iInput2.substring(ALGORITHM_PREFIX.length());

      // fall through to the code that just checks two strings against each other
    }

    return MessageDigest.isEqual(digest(iInput1), digest(iInput2));
  }

  public String digest2String(final String iInput) {
    return byteArrayToHexStr(digest(iInput));
  }

  /**
   * Hashes the input string.
   *
   * @param iInput
   *          String to hash
   * @param iIncludeAlgorithm
   *          Include the algorithm used or not
   * @return
   */
  public String digest2String(final String iInput, final boolean iIncludeAlgorithm) {
    final String transformed = OSecurityManager.instance().digest2String(iInput);

    // OPTIMIZE STRING BUILDE CREATION PASSING THE SIZE
    final StringBuilder buffer = new StringBuilder(ALGORITHM_PREFIX.length() + transformed.length());
    if (iIncludeAlgorithm)
      buffer.append(ALGORITHM_PREFIX);

    buffer.append(transformed);

    return buffer.toString();
  }

  public synchronized byte[] digest(final String iInput) {
    if (iInput == null)
      return null;

    try {
      return md.digest(iInput.getBytes("UTF-8"));
    } catch (UnsupportedEncodingException e) {
      OLogManager.instance().error(this, "The requested encoding is not supported: cannot execute security checks", e,
          OConfigurationException.class);
    }
    return null;
  }

  public synchronized byte[] digest(byte[] iInput) {
    return md.digest(iInput);
  }

  public SecretKey generateKey(final String iAlgorithm, final int iKeySize) {
    KeyGenerator kg;
    try {
      kg = KeyGenerator.getInstance(iAlgorithm);
      kg.init(iKeySize);
      return kg.generateKey();
    } catch (Exception e) {
      throw new OSecurityException("Error on generating key for algorithm: " + iAlgorithm, e);
    }
  }

  public SecretKey createKey(final String iAlgorithm, final byte[] iKey) throws OSecurityAccessException {
    return new SecretKeySpec(iKey, iAlgorithm);
  }

  public byte[] encrypt(final String iAlgorithm, final Key iKey, final byte[] iData) throws OSecurityAccessException {
    Cipher c;
    try {
      c = Cipher.getInstance(iAlgorithm);
      c.init(Cipher.ENCRYPT_MODE, iKey);
      return c.doFinal(iData);
    } catch (Exception e) {
      throw new OSecurityException("Error on encrypting data", e);
    }
  }

  public byte[] decrypt(final String iAlgorithm, final Key iKey, final byte[] iData) throws OSecurityAccessException {
    Cipher c;
    try {
      c = Cipher.getInstance(iAlgorithm);
      c.init(Cipher.DECRYPT_MODE, iKey);
      return c.doFinal(iData);
    } catch (Exception e) {
      throw new OSecurityException("Error on decrypting data", e);
    }
  }

  public String createSHA256(final String iInput) {
    return byteArrayToHexStr(digestSHA256(iInput));
  }

  public synchronized byte[] digestSHA256(final String iInput) {
    if (iInput == null)
      return null;

    try {
      return md.digest(iInput.getBytes("UTF-8"));
    } catch (UnsupportedEncodingException e) {
      final String message = "The requested encoding is not supported: cannot execute security checks";
      OLogManager.instance().error(this, message, e);

      throw new OConfigurationException(message, e);
    }
  }

}
