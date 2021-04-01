package com.orientechnologies.orient.server.network;

import com.orientechnologies.orient.server.security.OSelfSignedCertificate;
import com.orientechnologies.orient.server.security.SwitchToDefaultParamsException;
import java.io.*;
import java.security.*;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

public class OServerSSLCertificateManager {

  private char[] keyStorePass;
  private File keyStoreFile;
  private KeyStore keyStore;
  private OServerSSLSocketFactory oSSLSocketInfo = null;
  private OSelfSignedCertificate oSelfSignedCertificate = null;
  private KeyStore trustStore;
  private char[] trustStorePass;
  private File trustStoreFile;

  private OServerSSLCertificateManager() {}

  public OServerSSLCertificateManager(
      OServerSSLSocketFactory oServerSSLSocketFactory,
      KeyStore keyStore,
      File keyStoreFile,
      char[] keyStorePass) {
    oSSLSocketInfo = oServerSSLSocketFactory;
    this.keyStore = keyStore;
    this.keyStoreFile = keyStoreFile;
    this.keyStorePass = keyStorePass;
  }

  public static OServerSSLCertificateManager getInstance() {
    return new OServerSSLCertificateManager();
  }

  public static OServerSSLCertificateManager getInstance(
      OServerSSLSocketFactory oServerSSLSocketFactory,
      KeyStore keyStore,
      File keyStoreFile,
      char[] keyStorePass) {
    return new OServerSSLCertificateManager(
        oServerSSLSocketFactory, keyStore, keyStoreFile, keyStorePass);
  }

  public void loadKeyStoreForSSLSocket() throws Exception {
    try {
      if (!keyStoreFile.exists()) initKeyStore(this.keyStoreFile, this.keyStore, this.keyStorePass);
      else loadKeyStore(this.keyStoreFile, this.keyStore, this.keyStorePass);
      this.checkKeyStoreContentValidity();

    } catch (IOException e) {
      // the keystore file is corrupt
      throw e;
    } catch (CertificateException e) {
      // the content of keystore is not compliant....
      this.reactToCerificateLack();
    } catch (NoSuchAlgorithmException e) {
      // the chosen algorithm is wrong
      throw e;
    }
  }

  public void loadTrustStoreForSSLSocket(
      KeyStore trustStore, File trustStoreFile, char[] trustStorePass) throws Exception {

    this.trustStore = trustStore;
    this.trustStoreFile = trustStoreFile;
    this.trustStorePass = trustStorePass;

    try {

      if (!trustStoreFile.exists()) initKeyStore(trustStoreFile, trustStore, trustStorePass);
      else loadKeyStore(trustStoreFile, trustStore, trustStorePass);

    } catch (CertificateException e) {
      // forecatst of initKeyStore throw
    } catch (IOException e) {
      // the keystore file is corrupt
      throw e;
    } finally {
      if (this.oSelfSignedCertificate != null)
        trustCertificate(
            this.trustStoreFile,
            this.trustStore,
            this.trustStorePass,
            this.oSelfSignedCertificate.getCertificate_name(),
            this.oSelfSignedCertificate.getCertificate());
    }
  }

  public void checkKeyStoreContentValidity() throws CertificateException, KeyStoreException {
    if (!this.keyStore.aliases().hasMoreElements())
      throw new CertificateException("the KeyStore is empty");
  }

  public void reactToCerificateLack() throws Exception {
    try {

      if (this.oSelfSignedCertificate == null) this.initOSelfSignedCertificateParameters();

      autoGenerateSelfSignedX509Cerificate(this.oSelfSignedCertificate);

      storeCertificate(
          this.oSelfSignedCertificate.getCertificate(),
          this.oSelfSignedCertificate.getPrivateKey(),
          this.oSelfSignedCertificate.getCertificate_name(),
          this.keyStorePass,
          this.keyStoreFile,
          this.keyStore,
          this.keyStorePass);

    } catch (CertificateException e) {
      e.printStackTrace();
    } catch (Exception e) {
      throw e;
    }
  }

  private void initOSelfSignedCertificateParameters() {
    this.oSelfSignedCertificate = new OSelfSignedCertificate();
    this.oSelfSignedCertificate.setAlgorithm(OSelfSignedCertificate.DEFAULT_CERTIFICATE_ALGORITHM);
    this.oSelfSignedCertificate.setCertificate_name(
        OSelfSignedCertificate.DEFAULT_CERTIFICATE_NAME);
    try {
      this.oSelfSignedCertificate.setCertificate_SN(
          0); // trick to force it to conpute a random BigInteger
    } catch (SwitchToDefaultParamsException e) {
    }
    this.oSelfSignedCertificate.setCertificate_pwd(null);
    this.oSelfSignedCertificate.setKey_size(OSelfSignedCertificate.DEFAULT_CERTIFICATE_KEY_SIZE);
    this.oSelfSignedCertificate.setOwner_FDN(OSelfSignedCertificate.DEFAULT_CERTIFICATE_OWNER);
    this.oSelfSignedCertificate.setValidity(OSelfSignedCertificate.DEFAULT_CERTIFICATE_VALIDITY);
  }

  public static OSelfSignedCertificate autoGenerateSelfSignedX509Cerificate(
      OSelfSignedCertificate oCert)
      throws SwitchToDefaultParamsException, NoSuchAlgorithmException, CertificateException,
          NoSuchProviderException, InvalidKeyException, SignatureException {
    oCert.generateCertificateKeyPair();
    oCert.composeSelfSignedCertificate();
    oCert.checkThisCertificate();

    return oCert;
  }

  public static void initKeyStore(
      File keyStore_FilePointer, KeyStore keyStore_instance, char[] ks_pwd)
      throws IOException, CertificateException, NoSuchAlgorithmException {
    FileOutputStream ks_FOs = null;
    try {

      //            ks_FOs = new FileOutputStream(keyStore_FilePointer);

      if (!keyStore_FilePointer.exists()) keyStore_instance.load(null, null);

      //            keyStore_instance.load(null, ks_pwd);

      //            keyStore_instance.store(ks_FOs, ks_pwd);

    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    } finally {
      //            ks_FOs.close();
      throw new CertificateException("the KeyStore is empty");
    }
  }

  public static void loadKeyStore(
      File keyStore_FilePointer, KeyStore keyStore_instance, char[] ks_pwd)
      throws IOException, CertificateException, NoSuchAlgorithmException {

    FileInputStream ks_FIs = null;
    try {
      ks_FIs = new FileInputStream(keyStore_FilePointer);

      keyStore_instance.load(ks_FIs, ks_pwd);

    } catch (FileNotFoundException e) {
      e.printStackTrace();
      throw e;
    } finally {
      ks_FIs.close();
    }
  }

  public static void storeCertificate(
      X509Certificate cert,
      PrivateKey key,
      String cert_name,
      char[] cert_pwd,
      File keyStore_FilePointer,
      KeyStore keyStore_instance,
      char[] ks_pwd)
      throws KeyStoreException, IOException, CertificateException, NoSuchAlgorithmException {

    FileOutputStream ks_FOs = null;
    try {
      ks_FOs = new FileOutputStream(keyStore_FilePointer, true);

      keyStore_instance.setKeyEntry(
          cert_name, key, cert_pwd, new java.security.cert.Certificate[] {cert});

      keyStore_instance.store(ks_FOs, ks_pwd);

    } catch (FileNotFoundException e) {
      e.printStackTrace();
      throw e;
    } finally {
      ks_FOs.close();
    }
  }

  public static void trustCertificate(
      File keyStore_FilePointer,
      KeyStore keyStore_instance,
      char[] ks_pwd,
      String cert_name,
      X509Certificate cert)
      throws KeyStoreException, IOException, CertificateException, NoSuchAlgorithmException {

    FileOutputStream ks_FOs = null;
    try {
      ks_FOs = new FileOutputStream(keyStore_FilePointer, true);

      keyStore_instance.setCertificateEntry(cert_name, cert);

      keyStore_instance.store(ks_FOs, ks_pwd);

    } catch (FileNotFoundException e) {
      e.printStackTrace();
      throw e;
    } finally {
      ks_FOs.close();
    }
  }
}
