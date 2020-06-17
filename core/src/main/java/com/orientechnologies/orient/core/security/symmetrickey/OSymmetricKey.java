/*
 *
 *  *  Copyright 2016 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientdb.com
 *
 */
package com.orientechnologies.orient.core.security.symmetrickey;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.orient.core.exception.OSecurityException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.io.BufferedWriter;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.spec.KeySpec;
import java.util.Base64;
import java.util.UUID;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;

/**
 * Implements a symmetric key utility class that can create default keys and keys from a String, a
 * file, a KeyStore, and from the OSymmetricKeyConfig interface.
 *
 * <p>Static creation methods are provided for each type: OSymmetricKey.fromConfig()
 * OSymmetricKey.fromString() OSymmetricKey.fromFile() OSymmetricKey.fromStream()
 * OSymmetricKey.fromKeystore()
 *
 * <p>The encrypt() methods return a specialized Base64-encoded JSON document with these properties
 * (depending on the cipher transform): "algorithm", "transform", "iv", "payload"
 *
 * <p>The decrypt() and decryptAsString() methods accept the Base64-encoded JSON document.
 *
 * <p>A symmetric key credential interceptor is provided (OSymmetricKeyCI) as well as several
 * authenticators: OSecuritySymmetricKeyAuth, OSystemSymmetricKeyAuth
 *
 * @author S. Colin Leister
 */
public class OSymmetricKey {
  // These are just defaults.
  private String seedAlgorithm = "PBKDF2WithHmacSHA1";
  private String seedPhrase = UUID.randomUUID().toString();
  // Holds the length of the salt byte array.
  private int saltLength = 64;
  // Holds the default number of iterations used.  This may be overridden in the configuration.
  private int iteration = 65536;
  private String secretKeyAlgorithm = "AES";
  private String defaultCipherTransformation = "AES/CBC/PKCS5Padding";
  // Holds the size of the key (in bits).
  private int keySize = 128;

  private SecretKey secretKey;

  // Getters
  public String getDefaultCipherTransform(final String transform) {
    return defaultCipherTransformation;
  }

  public int getIteration(int iteration) {
    return iteration;
  }

  public String getKeyAlgorithm(final String algorithm) {
    return secretKeyAlgorithm;
  }

  public int getKeySize(int bits) {
    return keySize;
  }

  public int getSaltLength(int length) {
    return saltLength;
  }

  public String getSeedAlgorithm(final String algorithm) {
    return seedAlgorithm;
  }

  public String getSeedPhrase(final String phrase) {
    return seedPhrase;
  }

  // Setters
  public OSymmetricKey setDefaultCipherTransform(final String transform) {
    defaultCipherTransformation = transform;
    return this;
  }

  public OSymmetricKey setIteration(int iteration) {
    this.iteration = iteration;
    return this;
  }

  public OSymmetricKey setKeyAlgorithm(final String algorithm) {
    secretKeyAlgorithm = algorithm;
    return this;
  }

  public OSymmetricKey setKeySize(int bits) {
    keySize = bits;
    return this;
  }

  public OSymmetricKey setSaltLength(int length) {
    saltLength = length;
    return this;
  }

  public OSymmetricKey setSeedAlgorithm(final String algorithm) {
    seedAlgorithm = algorithm;
    return this;
  }

  public OSymmetricKey setSeedPhrase(final String phrase) {
    seedPhrase = phrase;
    return this;
  }

  public OSymmetricKey() {
    create();
  }

  /** Creates a key based on the algorithm, transformation, and key size specified. */
  public OSymmetricKey(
      final String secretKeyAlgorithm, final String cipherTransform, final int keySize) {
    this.secretKeyAlgorithm = secretKeyAlgorithm;
    this.defaultCipherTransformation = cipherTransform;
    this.keySize = keySize;

    create();
  }

  /** Uses the specified SecretKey as the private key and sets key algorithm from the SecretKey. */
  public OSymmetricKey(final SecretKey secretKey) throws OSecurityException {
    if (secretKey == null)
      throw new OSecurityException("OSymmetricKey(SecretKey) secretKey is null");

    this.secretKey = secretKey;
    this.secretKeyAlgorithm = secretKey.getAlgorithm();
  }

  /** Sets the SecretKey based on the specified algorithm and Base64 key specified. */
  public OSymmetricKey(final String algorithm, final String base64Key) throws OSecurityException {
    this.secretKeyAlgorithm = algorithm;

    try {
      final byte[] keyBytes = OSymmetricKey.convertFromBase64(base64Key);

      this.secretKey = new SecretKeySpec(keyBytes, secretKeyAlgorithm);
    } catch (Exception ex) {
      throw OException.wrapException(
          new OSecurityException("OSymmetricKey.OSymmetricKey() Exception: " + ex.getMessage()),
          ex);
    }
  }

  protected void create() {
    try {
      SecureRandom secureRandom = new SecureRandom();
      // ** This is actually not needed and will block for a long time on many operating systems.
      //    byte[] salt = secureRandom.generateSeed(saltLength);
      byte[] salt = new byte[saltLength];
      secureRandom.nextBytes(salt);

      KeySpec keySpec = new PBEKeySpec(seedPhrase.toCharArray(), salt, iteration, keySize);

      SecretKeyFactory factory = SecretKeyFactory.getInstance(seedAlgorithm);
      SecretKey tempKey = factory.generateSecret(keySpec);

      secretKey = new SecretKeySpec(tempKey.getEncoded(), secretKeyAlgorithm);
    } catch (Exception ex) {
      throw new OSecurityException("OSymmetricKey.create() Exception: " + ex);
    }
  }

  /** Returns the secret key algorithm portion of the cipher transformation. */
  protected static String separateAlgorithm(final String cipherTransform) {
    String[] array = cipherTransform.split("/");

    if (array.length > 1) return array[0];

    return null;
  }

  /** Creates an OSymmetricKey from an OSymmetricKeyConfig interface. */
  public static OSymmetricKey fromConfig(final OSymmetricKeyConfig keyConfig) {
    if (keyConfig.usesKeyString()) {
      return fromString(keyConfig.getKeyAlgorithm(), keyConfig.getKeyString());
    } else if (keyConfig.usesKeyFile()) {
      return fromFile(keyConfig.getKeyAlgorithm(), keyConfig.getKeyFile());
    } else if (keyConfig.usesKeystore()) {
      return fromKeystore(
          keyConfig.getKeystoreFile(),
          keyConfig.getKeystorePassword(),
          keyConfig.getKeystoreKeyAlias(),
          keyConfig.getKeystoreKeyPassword());
    } else {
      throw new OSecurityException("OSymmetricKey(OSymmetricKeyConfig) Invalid configuration");
    }
  }

  /** Creates an OSymmetricKey from a Base64 key. */
  public static OSymmetricKey fromString(final String algorithm, final String base64Key) {
    return new OSymmetricKey(algorithm, base64Key);
  }

  /** Creates an OSymmetricKey from a file containing a Base64 key. */
  public static OSymmetricKey fromFile(final String algorithm, final String path) {
    String base64Key = null;

    try {
      java.io.FileInputStream fis = null;

      try {
        fis = new java.io.FileInputStream(OSystemVariableResolver.resolveSystemVariables(path));

        return fromStream(algorithm, fis);
      } finally {
        if (fis != null) fis.close();
      }
    } catch (Exception ex) {
      throw OException.wrapException(
          new OSecurityException("OSymmetricKey.fromFile() Exception: " + ex.getMessage()), ex);
    }
  }

  /** Creates an OSymmetricKey from an InputStream containing a Base64 key. */
  public static OSymmetricKey fromStream(final String algorithm, final InputStream is) {
    String base64Key = null;

    try {
      base64Key = OIOUtils.readStreamAsString(is);
    } catch (Exception ex) {
      throw OException.wrapException(
          new OSecurityException("OSymmetricKey.fromStream() Exception: " + ex.getMessage()), ex);
    }

    return new OSymmetricKey(algorithm, base64Key);
  }

  /**
   * Creates an OSymmetricKey from a Java "JCEKS" KeyStore.
   *
   * @param path The location of the KeyStore file.
   * @param password The password for the KeyStore. May be null.
   * @param keyAlias The alias name of the key to be used from the KeyStore. Required.
   * @param keyPassword The password of the key represented by keyAlias. May be null.
   */
  public static OSymmetricKey fromKeystore(
      final String path, final String password, final String keyAlias, final String keyPassword) {
    OSymmetricKey sk = null;

    try {
      KeyStore ks = KeyStore.getInstance("JCEKS"); // JCEKS is required to hold SecretKey entries.

      java.io.FileInputStream fis = null;

      try {
        fis = new java.io.FileInputStream(OSystemVariableResolver.resolveSystemVariables(path));

        return fromKeystore(fis, password, keyAlias, keyPassword);
      } finally {
        if (fis != null) fis.close();
      }
    } catch (Exception ex) {
      throw OException.wrapException(
          new OSecurityException("OSymmetricKey.fromKeystore() Exception: " + ex.getMessage()), ex);
    }
  }

  /**
   * Creates an OSymmetricKey from a Java "JCEKS" KeyStore.
   *
   * @param is The InputStream used to load the KeyStore.
   * @param password The password for the KeyStore. May be null.
   * @param keyAlias The alias name of the key to be used from the KeyStore. Required.
   * @param keyPassword The password of the key represented by keyAlias. May be null.
   */
  public static OSymmetricKey fromKeystore(
      final InputStream is,
      final String password,
      final String keyAlias,
      final String keyPassword) {
    OSymmetricKey sk = null;

    try {
      KeyStore ks = KeyStore.getInstance("JCEKS"); // JCEKS is required to hold SecretKey entries.

      char[] ksPasswdChars = null;

      if (password != null) ksPasswdChars = password.toCharArray();

      ks.load(is, ksPasswdChars); // ksPasswdChars may be null.

      char[] ksKeyPasswdChars = null;

      if (keyPassword != null) ksKeyPasswdChars = keyPassword.toCharArray();

      KeyStore.ProtectionParameter protParam =
          new KeyStore.PasswordProtection(ksKeyPasswdChars); // ksKeyPasswdChars may be null.

      KeyStore.SecretKeyEntry skEntry = (KeyStore.SecretKeyEntry) ks.getEntry(keyAlias, protParam);

      if (skEntry == null)
        throw new OSecurityException("SecretKeyEntry is null for key alias: " + keyAlias);

      SecretKey secretKey = skEntry.getSecretKey();

      sk = new OSymmetricKey(secretKey);
    } catch (Exception ex) {
      throw OException.wrapException(
          new OSecurityException("OSymmetricKey.fromKeystore() Exception: " + ex.getMessage()), ex);
    }

    return sk;
  }

  /** Returns the internal SecretKey as a Base64 String. */
  public String getBase64Key() {
    if (secretKey == null)
      throw new OSecurityException("OSymmetricKey.getBase64Key() SecretKey is null");

    return convertToBase64(secretKey.getEncoded());
  }

  protected static String convertToBase64(final byte[] bytes) {
    String result = null;

    try {
      result = Base64.getEncoder().encodeToString(bytes);
    } catch (Exception ex) {
      OLogManager.instance().error(null, "convertToBase64()", ex);
    }

    return result;
  }

  protected static byte[] convertFromBase64(final String base64) {
    byte[] result = null;

    try {
      if (base64 != null) {
        result = Base64.getDecoder().decode(base64.getBytes("UTF8"));
      }
    } catch (Exception ex) {
      OLogManager.instance().error(null, "convertFromBase64()", ex);
    }

    return result;
  }

  /**
   * This is a convenience method that takes a String argument, encodes it as Base64, then calls
   * encrypt(byte[]).
   *
   * @param value The String to be encoded to Base64 then encrypted.
   * @return A Base64-encoded JSON document.
   */
  public String encrypt(final String value) {
    try {
      return encrypt(value.getBytes("UTF8"));
    } catch (Exception ex) {
      throw OException.wrapException(
          new OSecurityException("OSymmetricKey.encrypt() Exception: " + ex.getMessage()), ex);
    }
  }

  /**
   * This is a convenience method that takes a String argument, encodes it as Base64, then calls
   * encrypt(byte[]).
   *
   * @param transform The cipher transformation to use.
   * @param value The String to be encoded to Base64 then encrypted.
   * @return A Base64-encoded JSON document.
   */
  public String encrypt(final String transform, final String value) {
    try {
      return encrypt(transform, value.getBytes("UTF8"));
    } catch (Exception ex) {
      throw OException.wrapException(
          new OSecurityException("OSymmetricKey.encrypt() Exception: " + ex.getMessage()), ex);
    }
  }

  /**
   * This method encrypts an array of bytes.
   *
   * @param bytes The array of bytes to be encrypted.
   * @return The encrypted bytes as a Base64-encoded JSON document or null if unsuccessful.
   */
  public String encrypt(final byte[] bytes) {
    return encrypt(defaultCipherTransformation, bytes);
  }

  /**
   * This method encrypts an array of bytes.
   *
   * @param transform The cipher transformation to use.
   * @param bytes The array of bytes to be encrypted.
   * @return The encrypted bytes as a Base64-encoded JSON document or null if unsuccessful.
   */
  public String encrypt(final String transform, final byte[] bytes) {
    String encodedJSON = null;

    if (secretKey == null)
      throw new OSecurityException("OSymmetricKey.encrypt() SecretKey is null");
    if (transform == null)
      throw new OSecurityException(
          "OSymmetricKey.encrypt() Cannot determine cipher transformation");

    try {
      // Throws NoSuchAlgorithmException and NoSuchPaddingException.
      Cipher cipher = Cipher.getInstance(transform);

      // If the cipher transformation requires an initialization vector then init() will create a
      // random one.
      // (Use cipher.getIV() to retrieve the IV, if it exists.)
      cipher.init(Cipher.ENCRYPT_MODE, secretKey);

      // If the cipher does not use an IV, this will be null.
      byte[] initVector = cipher.getIV();

      //      byte[] initVector =
      // encCipher.getParameters().getParameterSpec(IvParameterSpec.class).getIV();

      byte[] encrypted = cipher.doFinal(bytes);

      encodedJSON = encodeJSON(encrypted, initVector);
    } catch (Exception ex) {
      throw OException.wrapException(
          new OSecurityException("OSymmetricKey.encrypt() Exception: " + ex.getMessage()), ex);
    }

    return encodedJSON;
  }

  protected String encodeJSON(final byte[] encrypted, final byte[] initVector) {
    String encodedJSON = null;

    String encryptedBase64 = convertToBase64(encrypted);
    String initVectorBase64 = null;

    if (initVector != null) initVectorBase64 = convertToBase64(initVector);

    // Create the JSON document.
    StringBuffer sb = new StringBuffer();
    sb.append("{");
    sb.append("\"algorithm\":\"");
    sb.append(secretKeyAlgorithm);
    sb.append("\",\"transform\":\"");
    sb.append(defaultCipherTransformation);
    sb.append("\",\"payload\":\"");
    sb.append(encryptedBase64);
    sb.append("\"");

    if (initVectorBase64 != null) {
      sb.append(",\"iv\":\"");
      sb.append(initVectorBase64);
      sb.append("\"");
    }

    sb.append("}");

    try {
      // Convert the JSON document to Base64, for a touch more obfuscation.
      encodedJSON = convertToBase64(sb.toString().getBytes("UTF8"));

    } catch (Exception ex) {
      OLogManager.instance().error(this, "Convert to Base64 exception", ex);
    }

    return encodedJSON;
  }

  /**
   * This method decrypts the Base64-encoded JSON document using the specified algorithm and cipher
   * transformation.
   *
   * @param encodedJSON The Base64-encoded JSON document.
   * @return The decrypted array of bytes as a UTF8 String or null if not successful.
   */
  public String decryptAsString(final String encodedJSON) {
    try {
      byte[] decrypted = decrypt(encodedJSON);
      return new String(decrypted, "UTF8");
    } catch (Exception ex) {
      throw OException.wrapException(
          new OSecurityException("OSymmetricKey.decryptAsString() Exception: " + ex.getMessage()),
          ex);
    }
  }

  /**
   * This method decrypts the Base64-encoded JSON document using the specified algorithm and cipher
   * transformation.
   *
   * @param encodedJSON The Base64-encoded JSON document.
   * @return The decrypted array of bytes or null if unsuccessful.
   */
  public byte[] decrypt(final String encodedJSON) {
    byte[] result = null;

    if (encodedJSON == null)
      throw new OSecurityException("OSymmetricKey.decrypt(String) encodedJSON is null");

    try {
      byte[] decoded = convertFromBase64(encodedJSON);

      if (decoded == null)
        throw new OSecurityException(
            "OSymmetricKey.decrypt(String) encodedJSON could not be decoded");

      String json = new String(decoded, "UTF8");

      // Convert the JSON content to an ODocument to make parsing it easier.
      final ODocument doc = new ODocument().fromJSON(json, "noMap");

      // Set a default in case the JSON document does not contain an "algorithm" property.
      String algorithm = secretKeyAlgorithm;

      if (doc.containsField("algorithm")) algorithm = doc.field("algorithm");

      // Set a default in case the JSON document does not contain a "transform" property.
      String transform = defaultCipherTransformation;

      if (doc.containsField("transform")) transform = doc.field("transform");

      String payloadBase64 = doc.field("payload");
      String ivBase64 = doc.field("iv");

      byte[] payload = null;
      byte[] iv = null;

      if (payloadBase64 != null) payload = convertFromBase64(payloadBase64);
      if (ivBase64 != null) iv = convertFromBase64(ivBase64);

      // Throws NoSuchAlgorithmException and NoSuchPaddingException.
      Cipher cipher = Cipher.getInstance(transform);

      if (iv != null) cipher.init(Cipher.DECRYPT_MODE, secretKey, new IvParameterSpec(iv));
      else cipher.init(Cipher.DECRYPT_MODE, secretKey);

      result = cipher.doFinal(payload);
    } catch (Exception ex) {
      throw OException.wrapException(
          new OSecurityException("OSymmetricKey.decrypt(String) Exception: " + ex.getMessage()),
          ex);
    }

    return result;
  }

  /** Saves the internal SecretKey to the specified OutputStream as a Base64 String. */
  public void saveToStream(final OutputStream os) {
    if (os == null)
      throw new OSecurityException("OSymmetricKey.saveToStream() OutputStream is null");

    try {
      final OutputStreamWriter osw = new OutputStreamWriter(os);
      try {
        final BufferedWriter writer = new BufferedWriter(osw);
        try {
          writer.write(getBase64Key());
        } finally {
          writer.close();
        }
      } finally {
        os.close();
      }
    } catch (Exception ex) {
      throw OException.wrapException(
          new OSecurityException("OSymmetricKey.saveToStream() Exception: " + ex.getMessage()), ex);
    }
  }

  /** Saves the internal SecretKey as a KeyStore. */
  public void saveToKeystore(
      final OutputStream os, final String ksPasswd, final String keyAlias, final String keyPasswd) {
    if (os == null)
      throw new OSecurityException("OSymmetricKey.saveToKeystore() OutputStream is null");
    if (ksPasswd == null)
      throw new OSecurityException("OSymmetricKey.saveToKeystore() Keystore Password is required");
    if (keyAlias == null)
      throw new OSecurityException("OSymmetricKey.saveToKeystore() Key Alias is required");
    if (keyPasswd == null)
      throw new OSecurityException("OSymmetricKey.saveToKeystore() Key Password is required");

    try {
      KeyStore ks = KeyStore.getInstance("JCEKS");

      char[] ksPasswdCA = ksPasswd.toCharArray();
      char[] keyPasswdCA = keyPasswd.toCharArray();

      // Create a new KeyStore by passing null.
      ks.load(null, ksPasswdCA);

      KeyStore.ProtectionParameter protParam = new KeyStore.PasswordProtection(keyPasswdCA);

      KeyStore.SecretKeyEntry skEntry = new KeyStore.SecretKeyEntry(secretKey);
      ks.setEntry(keyAlias, skEntry, protParam);

      // Save the KeyStore
      ks.store(os, ksPasswdCA);
    } catch (Exception ex) {
      throw OException.wrapException(
          new OSecurityException("OSymmetricKey.saveToKeystore() Exception: " + ex.getMessage()),
          ex);
    }
  }
}
