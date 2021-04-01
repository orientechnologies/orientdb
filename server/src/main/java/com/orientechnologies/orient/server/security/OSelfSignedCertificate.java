package com.orientechnologies.orient.server.security;

import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerConfiguration;
import java.io.*;
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

  //    public static final String PARAM_NETWORK_SSL_CLIENT_AUTH = "network.ssl.clientAuth";
  //    public static final String PARAM_NETWORK_SSL_KEYSTORE = "network.ssl.keyStore";
  //    public static final String PARAM_NETWORK_SSL_KEYSTORE_TYPE = "network.ssl.keyStoreType";
  //    public static final String PARAM_NETWORK_SSL_KEYSTORE_PASSWORD =
  // "network.ssl.keyStorePassword";
  //    public static final String PARAM_NETWORK_SSL_TRUSTSTORE = "network.ssl.trustStore";
  //    public static final String PARAM_NETWORK_SSL_TRUSTSTORE_TYPE = "network.ssl.trustStoreType";
  //    public static final String PARAM_NETWORK_SSL_TRUSTSTORE_PASSWORD =
  // "network.ssl.trustStorePassword";

  public static final String DEFAULT_CERTIFICATE_TYPE = "X.509";
  public static final String DEFAULT_CERTIFICATE_ALGORITHM = "RSA";
  public static final int DEFAULT_CERTIFICATE_KEY_SIZE = 2048;
  public static final int DEFAULT_CERTIFICATE_VALIDITY = 365;

  private static final String DEFAULT_KEYSTORE_PATH = "${ORIENTDB_HOME}";
  private static final String DEFAULT_KEYSTORE_TYPE = KeyStore.getDefaultType();
  private static final String DEFAULT_KEYSTORE_PWD = "";

  public static final String DEFAULT_CERTIFICATE_OWNER =
      "CN=SelfSigenedOrientDBtestOnly, OU=SAP HANA Core, O=SAP SE, L=Walldorf, C=DE";
  //    public static final String DEFAULT_CERTIFICATE_OWNER = "CN=localhost.localdomain";
  //    public static final String DEFAULT_CERTIFICATE_OWNER = "CN=C02WP0WUHV2Q.local";
  public static final String DEFAULT_CERTIFICATE_NAME = "ssl";

  private String clientAuth;

  private String algorithm;
  private int key_size;
  private int validity;
  private KeyPair key_pair = null;
  private X509Certificate certificate = null;

  private String certificate_name;
  private char[] certificate_pwd;
  private BigInteger certificate_SN;
  private String owner_FDN;

  private String keyStore;
  private String keyStoreType;
  private char[] keyStorePassword;

  private OServer iServer;
  private OServerConfiguration iConf;

  public OSelfSignedCertificate() {

    this.certificate_SN = computeRandomSerialNumber();
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

  public String getCertificate_name() {
    return certificate_name;
  }

  public void setCertificate_name(String certificate_name) {
    this.certificate_name = certificate_name;
  }

  public char[] getCertificate_pwd() {
    return certificate_pwd;
  }

  public void setCertificate_pwd(char[] certificate_pwd) {
    this.certificate_pwd = certificate_pwd;
  }

  public BigInteger getCertificate_SN() {
    return certificate_SN;
  }

  public void setCertificate_SN(long certificate_SN) throws SwitchToDefaultParamsException {
    if (certificate_SN <= 11) {
      BigInteger sn = computeRandomSerialNumber();
      this.certificate_SN = sn;
      throw new SwitchToDefaultParamsException(
          "the value "
              + certificate_SN
              + " culd not be used as a Certificate Serial Nuber, the value will be set to:"
              + sn);
    } else this.certificate_SN = BigInteger.valueOf(certificate_SN);
  }

  public static BigInteger computeRandomSerialNumber() {
    SecureRandom sr = new SecureRandom();
    return BigInteger.valueOf(sr.nextLong());
  }

  public String getOwner_FDN() {
    return owner_FDN;
  }

  public void setOwner_FDN(String owner_FDN) {
    this.owner_FDN = owner_FDN;
  }

  public void setOwner_FDN(String CN, String OU, String O, String L, String C) {
    this.owner_FDN = "CN=" + CN + ", OU=" + OU + ", O=" + O + ", L=" + L + ", C=" + C;
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
      this.key_pair = computeKeyPair(this.algorithm, this.key_size);
    } catch (NoSuchAlgorithmException e) {
      this.key_pair = computeKeyPair(DEFAULT_CERTIFICATE_ALGORITHM, DEFAULT_CERTIFICATE_KEY_SIZE);
      SwitchToDefaultParamsException tmpe = new SwitchToDefaultParamsException();
      tmpe.addSuppressed(e);
      throw tmpe;
    }
  }

  public PublicKey getPublicKey() {
    if (key_pair == null) {
      throw new NullPointerException("generate the Key Pair");
    }
    return key_pair.getPublic();
  }

  public void composeSelfSignedCertificate() {
    try {
      this.certificate =
          generateSelfSignedCertificate(
              this.key_pair, this.validity, this.owner_FDN, this.certificate_SN);
    } catch (CertificateException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static X509Certificate generateSelfSignedCertificate(
      KeyPair key_pair, int validity, String owner_FDN, BigInteger cert_SN)
      throws CertificateException, IOException {

    X509CertImpl cert;

    //  Build the X.509 certificate content:
    X509CertInfo info = new X509CertInfo();
    X500Name owner;
    owner = new X500Name(owner_FDN);

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
    info.set(X509CertInfo.SERIAL_NUMBER, new CertificateSerialNumber(cert_SN));

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
    info.set(X509CertInfo.KEY, new CertificateX509Key(key_pair.getPublic()));

    // set certificate Signature ALGORITHM = RSA
    info.set(
        X509CertInfo.ALGORITHM_ID,
        new CertificateAlgorithmId(new AlgorithmId(AlgorithmId.sha256WithRSAEncryption_oid)));
    // set certificate Signature ALGORITHM = RSA
    //        info.set(X509CertInfo.ALGORITHM_ID,new CertificateAlgorithmId(new
    // AlgorithmId(AlgorithmId.sha256WithDSA_oid)));

    // Sign the cert to identify the algorithm that's used.
    cert = new X509CertImpl(info);

    try {
      cert.sign(key_pair.getPrivate(), "SHA256withRSA");
      //            cert.sign(key_pair.getPrivate(),"SHA1withDSA");
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
      cert.sign(key_pair.getPrivate(), "SHA256withRSA");
      cert.verify(key_pair.getPublic());
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

  // original signature    static String[] generate(String fqdn, KeyPair keypair, SecureRandom
  // random, Date notBefore, Date notAfter)
  //    // Snippet sample copied by codata.com
  //    static void generate(String fqdn, KeyPair keypair, SecureRandom random, Date notBefore, Date
  // notAfter)
  //            throws Exception {
  //        PrivateKey key = keypair.getPrivate();
  //        // Prepare the information required for generating an X.509 certificate.
  //        X509CertInfo info = new X509CertInfo();
  //        X500Name owner = new X500Name("CN=" + fqdn);
  //        info.set(X509CertInfo.VERSION, new CertificateVersion(CertificateVersion.V3));
  //        info.set(X509CertInfo.SERIAL_NUMBER, new CertificateSerialNumber(new BigInteger(64,
  // random)));
  //        try {
  //            info.set(X509CertInfo.SUBJECT, new CertificateSubjectName(owner));
  //        } catch (CertificateException ignore) {
  //            info.set(X509CertInfo.SUBJECT, owner);
  //        }
  //        try {
  //            info.set(X509CertInfo.ISSUER, new CertificateIssuerName(owner));
  //        } catch (CertificateException ignore) {
  //            info.set(X509CertInfo.ISSUER, owner);
  //        }
  //        info.set(X509CertInfo.VALIDITY, new CertificateValidity(notBefore, notAfter));
  //        info.set(X509CertInfo.KEY, new CertificateX509Key(keypair.getPublic()));
  //        info.set(X509CertInfo.ALGORITHM_ID,
  //                new CertificateAlgorithmId(new
  // AlgorithmId(AlgorithmId.sha256WithRSAEncryption_oid)));
  //        // Sign the cert to identify the algorithm that's used.
  //        X509CertImpl cert = new X509CertImpl(info);
  //        cert.sign(key, "SHA256withRSA");
  //        // Update the algorithm and sign again.
  //        info.set(CertificateAlgorithmId.NAME + '.' + CertificateAlgorithmId.ALGORITHM,
  // cert.get(X509CertImpl.SIG_ALG));
  //        cert = new X509CertImpl(info);
  //        cert.sign(key, "SHA256withRSA");
  //        cert.verify(keypair.getPublic());
  //        /*
  //        OUTPUT should be somenthing like this...
  //
  //                String cert_content = "Owner: CN=OrientDB, OU=SAP HANA Core, O=SAP SE,
  // L=Walldorf, C=DE\n" +
  //                "Issuer: CN=OrientDB, OU=SAP HANA Core, O=SAP SE, L=Walldorf, C=DE\n" +
  //                "Serial number: 58b9fe25\n" +
  //                "Valid from: Mon Mar 01 17:48:53 CET 2021 until: Tue Mar 01 17:48:53 CET 2022\n"
  // +
  //                "Certificate fingerprints:\n" +
  //                "         MD5:  EC:C3:E5:D3:B3:50:FB:59:EA:51:D6:CF:EF:E5:11:DA\n" +
  //                "         SHA1: 0F:A9:DA:DD:ED:F9:3E:5B:5D:2D:A7:D8:74:08:DF:29:9E:0E:82:0B\n" +
  //                "         SHA256:
  // 42:9C:B9:0F:AB:0A:58:AB:42:C3:DB:39:43:EA:61:B2:C8:F8:72:D0:8F:99:B4:95:81:21:2C:E0:96:3A:60:68\n" +
  //                "Signature algorithm name: SHA256withDSA\n" +
  //                "Subject Public Key Algorithm: 1024-bit DSA key\n" +
  //                "Version: 3\n" +
  //                "\n" +
  //                "Extensions: \n" +
  //                "\n" +
  //                "#1: ObjectId: 2.5.29.14 Criticality=false\n" +
  //                "SubjectKeyIdentifier [\n" +
  //                "KeyIdentifier [\n" +
  //                "0000: EB AB 15 7F 77 D3 CC 06   84 BF 72 B3 1B 2E 48 EE  ....w.....r...H.\n" +
  //                "0010: 2F 24 40 7E                                        /$@.\n" +
  //                "]\n" +
  //                "]\n";
  //         */
  //
  ////        return newSelfSignedCertificate(fqdn, key, cert);
  //    }

  public X509Certificate getCertificate() throws CertificateException {

    if (this.certificate == null) {
      CertificateException cEx =
          new CertificateException(
              "The Self-Signed Certificate han not been genetated! You have to invoke the composeSelfSignedCertificate() before get it.");
      throw cEx;
    }
    return this.certificate;
  }

  public static boolean checkCertificate(X509Certificate cert, PublicKey public_key, Date date)
      throws NoSuchProviderException, CertificateException, NoSuchAlgorithmException,
          InvalidKeyException, SignatureException {
    cert.checkValidity(date);
    cert.verify(public_key);
    return true;
  }

  public static boolean checkCertificate(X509Certificate cert, PublicKey public_key)
      throws NoSuchProviderException, CertificateException, NoSuchAlgorithmException,
          InvalidKeyException, SignatureException {
    return checkCertificate(cert, public_key, new Date(System.currentTimeMillis()));
  }

  public boolean checkThisCertificate()
      throws NoSuchAlgorithmException, CertificateException, NoSuchProviderException,
          InvalidKeyException, SignatureException {
    return checkCertificate(
        this.certificate, this.key_pair.getPublic(), new Date(System.currentTimeMillis()));
  }

  public PrivateKey getPrivateKey() {
    return this.key_pair.getPrivate();
  }
}
