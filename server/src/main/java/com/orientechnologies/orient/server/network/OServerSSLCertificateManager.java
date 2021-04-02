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
            this.oSelfSignedCertificate.getCertificateName(),
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
          this.oSelfSignedCertificate.getCertificateName(),
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
    this.oSelfSignedCertificate.setCertificateName(OSelfSignedCertificate.DEFAULT_CERTIFICATE_NAME);
    try {
      this.oSelfSignedCertificate.setCertificateSN(
          0); // trick to force it to conpute a random BigInteger
    } catch (SwitchToDefaultParamsException e) {
    }
    this.oSelfSignedCertificate.setCertificatePwd(null);
    this.oSelfSignedCertificate.setKey_size(OSelfSignedCertificate.DEFAULT_CERTIFICATE_KEY_SIZE);
    this.oSelfSignedCertificate.setOwnerFDN(OSelfSignedCertificate.DEFAULT_CERTIFICATE_OWNER);
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
      File keyStoreFilePointer, KeyStore keyStoreInstance, char[] ks_pwd)
      throws IOException, CertificateException, NoSuchAlgorithmException {
    try {
      if (!keyStoreFilePointer.exists()) keyStoreInstance.load(null, null);
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    } finally {
      throw new CertificateException("the KeyStore is empty");
    }
  }

  public static void loadKeyStore(
      File keyStoreFilePointer, KeyStore keyStoreInstance, char[] ks_pwd)
      throws IOException, CertificateException, NoSuchAlgorithmException {

    FileInputStream ksFIs = null;
    try {
      ksFIs = new FileInputStream(keyStoreFilePointer);

      keyStoreInstance.load(ksFIs, ks_pwd);

    } catch (FileNotFoundException e) {
      e.printStackTrace();
      throw e;
    } finally {
      ksFIs.close();
    }
  }

  public static void storeCertificate(
      X509Certificate cert,
      PrivateKey key,
      String certName,
      char[] certPwd,
      File keyStore_FilePointer,
      KeyStore keyStore_instance,
      char[] ksPwd)
      throws KeyStoreException, IOException, CertificateException, NoSuchAlgorithmException {

    FileOutputStream ksFOs = null;
    try {
      ksFOs = new FileOutputStream(keyStore_FilePointer, true);

      keyStore_instance.setKeyEntry(
          certName, key, certPwd, new java.security.cert.Certificate[] {cert});

      keyStore_instance.store(ksFOs, ksPwd);

    } catch (FileNotFoundException e) {
      e.printStackTrace();
      throw e;
    } finally {
      ksFOs.close();
    }
  }

  public static void trustCertificate(
      File keyStoreFilePointer,
      KeyStore keyStoreInstance,
      char[] ksPwd,
      String certName,
      X509Certificate cert)
      throws KeyStoreException, IOException, CertificateException, NoSuchAlgorithmException {

    FileOutputStream ksFOs = null;
    try {
      ksFOs = new FileOutputStream(keyStoreFilePointer, true);

      keyStoreInstance.setCertificateEntry(certName, cert);

      keyStoreInstance.store(ksFOs, ksPwd);

    } catch (FileNotFoundException e) {
      e.printStackTrace();
      throw e;
    } finally {
      ksFOs.close();
    }
  }
}
