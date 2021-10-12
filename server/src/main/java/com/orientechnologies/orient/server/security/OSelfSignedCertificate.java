package com.orientechnologies.orient.server.security;

import java.io.IOException;
import java.math.BigInteger;
import java.security.*;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Calendar;
import java.util.Date;
import sun.security.x509.*;

/**
 * @author Matteo Bollo (matteo.bollo-at-sap.com)
 * @since 24/02/2021
 *     <p>Class developed to generate self-signed certificate
 */
public class OSelfSignedCertificate<tmpLocalHost> {

  public static final String DEFAULT_CERTIFICATE_TYPE = "X.509";
  public static final String DEFAULT_CERTIFICATE_ALGORITHM = "RSA";
  public static final int DEFAULT_CERTIFICATE_KEY_SIZE = 2048;
  public static final int DEFAULT_CERTIFICATE_VALIDITY = 365;

  public static final String DEFAULT_CERTIFICATE_OWNER =
      "CN=SelfSigenedOrientDBtestOnly, OU=SAP HANA Core, O=SAP SE, L=Walldorf, C=DE";
  public static final String DEFAULT_CERTIFICATE_NAME = "ssl";

  private String algorithm;
  private int key_size;
  private int validity;
  private KeyPair keyPair = null;
  private X509Certificate certificate = null;

  private String certificateName;
  private char[] certificate_pwd;
  private BigInteger certificateSN;
  private String ownerFDN;

  public OSelfSignedCertificate() {

    this.certificateSN = computeRandomSerialNumber();
  }

  public String getAlgorithm() {
    return algorithm;
  }

  public void setAlgorithm(String algorithm) {
    if ((algorithm == null) || (algorithm.isEmpty()))
      this.algorithm = DEFAULT_CERTIFICATE_ALGORITHM;
    else this.algorithm = algorithm;
  }

  public int getKey_size() {
    return key_size;
  }

  public void setKey_size(int key_size) {
    if (key_size >= 128) {
      this.key_size = key_size;
    } else {
      this.key_size = DEFAULT_CERTIFICATE_KEY_SIZE;
    }
  }

  public int getValidity() {
    return validity;
  }

  public void setValidity(int validity) {
    this.validity = validity;
  }

  public String getCertificateName() {
    return certificateName;
  }

  public void setCertificateName(String certificateName) {
    this.certificateName = certificateName;
  }

  public char[] getCertificatePwd() {
    return certificate_pwd;
  }

  public void setCertificatePwd(char[] certificatePwd) {
    this.certificate_pwd = certificatePwd;
  }

  public BigInteger getCertificateSN() {
    return certificateSN;
  }

  public void setCertificateSN(long certificateSN) throws SwitchToDefaultParamsException {
    if (certificateSN <= 11) {
      BigInteger sn = computeRandomSerialNumber();
      this.certificateSN = sn;
      throw new SwitchToDefaultParamsException(
          "the value "
              + certificateSN
              + " culd not be used as a Certificate Serial Nuber, the value will be set to:"
              + sn);
    } else this.certificateSN = BigInteger.valueOf(certificateSN);
  }

  public static BigInteger computeRandomSerialNumber() {
    SecureRandom sr = new SecureRandom();
    return BigInteger.valueOf(sr.nextLong());
  }

  public String getOwnerFDN() {
    return ownerFDN;
  }

  public void setOwnerFDN(String ownerFDN) {
    this.ownerFDN = ownerFDN;
  }

  public void setOwner_FDN(String CN, String OU, String O, String L, String C) {
    this.ownerFDN = "CN=" + CN + ", OU=" + OU + ", O=" + O + ", L=" + L + ", C=" + C;
  }

  /**
   * Generate and Return a key pair.
   *
   * <p>If this KeyPairGenerator has not been initialized explicitly, provider-specific defaults
   * will be used for the size and other (algorithm-specific) values of the generated keys.Our
   * People
   *
   * <p>This method will computes and returns a new key pair every time it is called.
   *
   * @throws NoSuchAlgorithmException if the algorithm String not match with the supported key
   *     generation schemes.
   * @return a new key pair
   */
  public static KeyPair computeKeyPair(String algorithm, int keySize)
      throws NoSuchAlgorithmException {

    KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance(algorithm);
    keyPairGenerator.initialize(keySize, new SecureRandom());
    KeyPair keyPair = keyPairGenerator.generateKeyPair();

    return keyPair;
  }

  /**
   * recompute a new key pair FOR INTERNAL OSelfSignedCertificate class USE.
   *
   * <p>This method is functionally equivalent to {@link #computeKeyPair
   * computeKeyPair(this.algorithm,this.key_size)}. It uses the value pair
   * (DEFAULT_CERTIFICATE_ALGORITHM,DEFAULT_CERTIFICATE_KEY_SIZE) if the setted fields are not
   * valid.
   *
   * @throws NoSuchAlgorithmException if the algorithm String not match with the supported key
   *     generation schemes.
   */
  public void generateCertificateKeyPair()
      throws NoSuchAlgorithmException, SwitchToDefaultParamsException {
    try {
      this.keyPair = computeKeyPair(this.algorithm, this.key_size);
    } catch (NoSuchAlgorithmException e) {
      this.keyPair = computeKeyPair(DEFAULT_CERTIFICATE_ALGORITHM, DEFAULT_CERTIFICATE_KEY_SIZE);
      SwitchToDefaultParamsException tmpe = new SwitchToDefaultParamsException();
      tmpe.addSuppressed(e);
      throw tmpe;
    }
  }

  public PublicKey getPublicKey() {
    if (keyPair == null) {
      throw new NullPointerException("generate the Key Pair");
    }
    return keyPair.getPublic();
  }

  public void composeSelfSignedCertificate() {
    try {
      this.certificate =
          generateSelfSignedCertificate(
              this.keyPair, this.validity, this.ownerFDN, this.certificateSN);
    } catch (CertificateException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (NoSuchAlgorithmException e) {
      e.printStackTrace();
    }
  }

  public static X509Certificate generateSelfSignedCertificate(
      KeyPair keypair, int validity, String ownerFDN, BigInteger certSN)
      throws CertificateException, IOException, NoSuchAlgorithmException {

    X509CertImpl cert;

    //  Build the X.509 certificate content:
    X509CertInfo info = new X509CertInfo();
    X500Name owner;
    owner = new X500Name(ownerFDN);

    // set certificate VERSION
    try {
      info.set(X509CertInfo.VERSION, new CertificateVersion(CertificateVersion.V3));
    } catch (IOException e) {
      try {
        info.set(X509CertInfo.VERSION, new CertificateVersion(CertificateVersion.V2));
      } catch (IOException ex) {
        info.set(X509CertInfo.VERSION, new CertificateVersion(CertificateVersion.V1));
      }
    }

    // set certificate SERIAL NUMBER
    info.set(X509CertInfo.SERIAL_NUMBER, new CertificateSerialNumber(certSN));

    // set certificate SUBJECT i.e. the owner of the certificate.
    try {
      info.set(X509CertInfo.SUBJECT, new CertificateSubjectName(owner));
    } catch (CertificateException ignore) {
      info.set(X509CertInfo.SUBJECT, owner);
    }
    // set certificate ISSUER equal to SBUJECT as it is a self-signed certificate.
    try {
      info.set(X509CertInfo.ISSUER, new CertificateIssuerName(owner));
    } catch (CertificateException ignore) {
      info.set(X509CertInfo.ISSUER, owner);
    }

    // set certificate VALIDITY from today to today+validity

    Date from, to;
    Calendar c = Calendar.getInstance();
    c.add(Calendar.DAY_OF_YEAR, 0);
    from = c.getTime();
    c.add(Calendar.DAY_OF_YEAR, validity);
    to = c.getTime();
    info.set(X509CertInfo.VALIDITY, new CertificateValidity(from, to));

    // set certificate PUBLIC_KEY
    info.set(X509CertInfo.KEY, new CertificateX509Key(keypair.getPublic()));

    // set certificate Signature ALGORITHM = RSA
    info.set(
        X509CertInfo.ALGORITHM_ID, new CertificateAlgorithmId(AlgorithmId.get("SHA256WithRSA")));

    // Sign the cert to identify the algorithm that's used.
    cert = new X509CertImpl(info);

    try {
      cert.sign(keypair.getPrivate(), "SHA256withRSA");
      //            cert.sign(keyPair.getPrivate(),"SHA1withDSA");
    } catch (NoSuchAlgorithmException e) {
      e.printStackTrace();
    } catch (InvalidKeyException e) {
      e.printStackTrace();
    } catch (NoSuchProviderException e) {
      e.printStackTrace();
    } catch (SignatureException e) {
      e.printStackTrace();
    }

    // Update the algorithm and sign again.
    info.set(
        CertificateAlgorithmId.NAME + '.' + CertificateAlgorithmId.ALGORITHM,
        cert.get(X509CertImpl.SIG_ALG));

    cert = new X509CertImpl(info);

    try {
      cert.sign(keypair.getPrivate(), "SHA256withRSA");
      cert.verify(keypair.getPublic());
    } catch (NoSuchAlgorithmException e) {
      e.printStackTrace();
    } catch (InvalidKeyException e) {
      e.printStackTrace();
    } catch (NoSuchProviderException e) {
      e.printStackTrace();
    } catch (SignatureException e) {
      e.printStackTrace();
    }

    return cert;
  }

  public X509Certificate getCertificate() throws CertificateException {

    if (this.certificate == null) {
      CertificateException cEx =
          new CertificateException(
              "The Self-Signed Certificate han not been genetated! You have to invoke the composeSelfSignedCertificate() before get it.");
      throw cEx;
    }
    return this.certificate;
  }

  public static boolean checkCertificate(X509Certificate cert, PublicKey publicKey, Date date)
      throws NoSuchProviderException, CertificateException, NoSuchAlgorithmException,
          InvalidKeyException, SignatureException {
    cert.checkValidity(date);
    cert.verify(publicKey);
    return true;
  }

  public static boolean checkCertificate(X509Certificate cert, PublicKey publicKey)
      throws NoSuchProviderException, CertificateException, NoSuchAlgorithmException,
          InvalidKeyException, SignatureException {
    return checkCertificate(cert, publicKey, new Date(System.currentTimeMillis()));
  }

  public boolean checkThisCertificate()
      throws NoSuchAlgorithmException, CertificateException, NoSuchProviderException,
          InvalidKeyException, SignatureException {
    return checkCertificate(
        this.certificate, this.keyPair.getPublic(), new Date(System.currentTimeMillis()));
  }

  public PrivateKey getPrivateKey() {
    return this.keyPair.getPrivate();
  }
}
