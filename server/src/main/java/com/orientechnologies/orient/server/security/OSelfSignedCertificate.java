package com.orientechnologies.orient.server.security;

import java.io.IOException;
import java.math.BigInteger;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.SignatureException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Calendar;
import java.util.Date;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;

/**
 * @author Matteo Bollo (matteo.bollo-at-sap.com)
 * @since 24/02/2021
 *     <p>Class developed to generate self-signed certificate
 */
public class OSelfSignedCertificate {

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
  private BigInteger certificateSN;
  private String ownerFDN;

  public OSelfSignedCertificate() {

    this.certificateSN = computeRandomSerialNumber();
  }

  public String getAlgorithm() {
    return algorithm;
  }

  public void setAlgorithm(String algorithm) {
    if ((algorithm == null) || (algorithm.isEmpty())) {
      this.algorithm = DEFAULT_CERTIFICATE_ALGORITHM;
    } else {
      this.algorithm = algorithm;
    }
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

  public void setValidity(int validity) {
    this.validity = validity;
  }

  public String getCertificateName() {
    return certificateName;
  }

  public void setCertificateName(String certificateName) {
    this.certificateName = certificateName;
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
    } else {
      this.certificateSN = BigInteger.valueOf(certificateSN);
    }
  }

  public static BigInteger computeRandomSerialNumber() {
    SecureRandom sr = new SecureRandom();
    return BigInteger.valueOf(sr.nextLong());
  }

  public void setOwnerFDN(String ownerFDN) {
    this.ownerFDN = ownerFDN;
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
   * @return a new key pair
   * @throws NoSuchAlgorithmException if the algorithm String not match with the supported key
   *     generation schemes.
   */
  public static KeyPair computeKeyPair(String algorithm, int keySize)
      throws NoSuchAlgorithmException {

    KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance(algorithm);
    keyPairGenerator.initialize(keySize, new SecureRandom());

    return keyPairGenerator.generateKeyPair();
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
    } catch (CertificateException | IOException | NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }

  public static X509Certificate generateSelfSignedCertificate(
      KeyPair keypair, int validity, String ownerFDN, BigInteger certSN)
      throws CertificateException, IOException, NoSuchAlgorithmException {

    X500Name owner;
    owner = new X500Name(ownerFDN);

    Date from, to;
    Calendar c = Calendar.getInstance();
    c.add(Calendar.DAY_OF_YEAR, 0);
    from = c.getTime();
    c.add(Calendar.DAY_OF_YEAR, validity);
    to = c.getTime();

    var certBuilder =
        new X509v3CertificateBuilder(
            owner,
            certSN,
            from,
            to,
            owner,
            SubjectPublicKeyInfo.getInstance(keypair.getPublic().getEncoded()));

    try {
      var certHolder =
          certBuilder.build(
              new JcaContentSignerBuilder("SHA256WithRSA").build(keypair.getPrivate()));
      return new JcaX509CertificateConverter().getCertificate(certHolder);
    } catch (OperatorCreationException e) {
      throw new RuntimeException(e);
    }
  }

  public X509Certificate getCertificate() throws CertificateException {

    if (this.certificate == null) {
      throw new CertificateException(
          "The Self-Signed Certificate han not been genetated! "
              + "You have to invoke the composeSelfSignedCertificate() before get it.");
    }
    return this.certificate;
  }

  public static void checkCertificate(X509Certificate cert, PublicKey publicKey, Date date)
      throws NoSuchProviderException,
          CertificateException,
          NoSuchAlgorithmException,
          InvalidKeyException,
          SignatureException {
    cert.checkValidity(date);
    cert.verify(publicKey);
  }

  public void checkThisCertificate()
      throws NoSuchAlgorithmException,
          CertificateException,
          NoSuchProviderException,
          InvalidKeyException,
          SignatureException {
    checkCertificate(
        this.certificate, this.keyPair.getPublic(), new Date(System.currentTimeMillis()));
  }

  public PrivateKey getPrivateKey() {
    return this.keyPair.getPrivate();
  }
}
