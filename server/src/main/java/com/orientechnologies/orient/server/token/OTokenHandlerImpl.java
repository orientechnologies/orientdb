package com.orientechnologies.orient.server.token;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.exception.OSystemException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OCommonConst;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.security.OSecurityUser;
import com.orientechnologies.orient.core.metadata.security.OToken;
import com.orientechnologies.orient.core.metadata.security.OTokenException;
import com.orientechnologies.orient.core.metadata.security.jwt.OJwtHeader;
import com.orientechnologies.orient.core.metadata.security.jwt.OJwtPayload;
import com.orientechnologies.orient.core.metadata.security.jwt.OKeyProvider;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.security.OSecurityManager;
import com.orientechnologies.orient.server.OClientConnection;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OTokenHandler;
import com.orientechnologies.orient.server.binary.impl.OBinaryToken;
import com.orientechnologies.orient.server.network.protocol.ONetworkProtocolData;

import javax.crypto.Mac;
import java.io.*;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Random;
import java.util.UUID;

/**
 * Created by emrul on 27/10/2014.
 *
 * @author Emrul Islam <emrul@emrul.com> Copyright 2014 Emrul Islam
 */
public class OTokenHandlerImpl implements OTokenHandler {
  public static final String ENCRYPTION_ALGORITHM_DEFAULT = "HmacSHA256";

  private static       String           algorithm      = ENCRYPTION_ALGORITHM_DEFAULT;
  private static final ThreadLocal<Mac> threadLocalMac = new MacThreadLocal();

  protected static final int                    JWT_DELIMITER  = '.';
  private                OBinaryTokenSerializer binarySerializer;
  private                long                   sessionInMills = 1000 * 60 * 60;              // 1 HOUR
  private                OKeyProvider           keyProvider;
  private                Random                 keyGenerator   = new Random();

  public OTokenHandlerImpl(OServer server) {
    byte[] key = null;
    String algorithm;
    Long sessionTimeout;

    OContextConfiguration config = server.getContextConfiguration();
    String configKey = config.getValueAsString(OGlobalConfiguration.NETWORK_TOKEN_SECRETKEY);
    if (configKey == null || configKey.length() == 0)
      configKey = config.getValueAsString(OGlobalConfiguration.OAUTH2_SECRETKEY);

    if (configKey != null && configKey.length() > 0)
      key = Base64.getUrlDecoder().decode(configKey);

    if (key == null)
      key = OSecurityManager.instance().digestSHA256(String.valueOf(keyGenerator.nextLong()));

    keyProvider = new DefaultKeyProvider(key);

    sessionTimeout = config.getValueAsLong(OGlobalConfiguration.NETWORK_TOKEN_EXPIRE_TIMEOUT);
    if (sessionTimeout != null)
      sessionInMills = sessionTimeout * 1000 * 60;

    algorithm = config.getValueAsString(OGlobalConfiguration.NETWORK_TOKEN_ENCRYPTION_ALGORITHM);
    if (algorithm != null)
      this.algorithm = algorithm;

    try {
      Mac.getInstance(algorithm);
    } catch (NoSuchAlgorithmException nsa) {
      throw new IllegalArgumentException("Can't find encryption algorithm '" + algorithm + "'", nsa);
    }

    this.binarySerializer = new OBinaryTokenSerializer(new String[] { "plocal", "memory" }, keyProvider.getKeys(),
        new String[] { this.algorithm }, new String[] { "OrientDB", "node" });
  }

  protected OTokenHandlerImpl() {

  }

  protected OTokenHandlerImpl(byte[] key, long sessionLength, String algorithm) {
    keyProvider = new DefaultKeyProvider(key);
    this.algorithm = algorithm;
    sessionInMills = sessionLength * 1000 * 60;
    this.binarySerializer = new OBinaryTokenSerializer(new String[] { "plocal", "memory" }, keyProvider.getKeys(),
        new String[] { this.algorithm }, new String[] { "OrientDB" });
  }

  @Override
  public OToken parseWebToken(byte[] tokenBytes) {
    JsonWebToken token = null;

    // / <header>.<payload>.<signature>
    int firstDot = -1, secondDot = -1;
    for (int x = 0; x < tokenBytes.length; x++) {
      if (tokenBytes[x] == JWT_DELIMITER) {
        if (firstDot == -1)
          firstDot = x; // stores reference to first '.' character in JWT token
        else {
          secondDot = x;
          break;
        }
      }
    }

    if (firstDot == -1)
      throw new RuntimeException("Token data too short: missed header");

    if (secondDot == -1)
      throw new RuntimeException("Token data too short: missed signature");
    ;
    final byte[] decodedHeader = Base64.getUrlDecoder().decode(ByteBuffer.wrap(tokenBytes, 0, firstDot)).array();
    final byte[] decodedPayload = Base64.getUrlDecoder()
        .decode(ByteBuffer.wrap(tokenBytes, firstDot + 1, secondDot - (firstDot + 1))).array();
    final byte[] decodedSignature = Base64.getUrlDecoder()
        .decode(ByteBuffer.wrap(tokenBytes, secondDot + 1, tokenBytes.length - (secondDot + 1))).array();

    final OrientJwtHeader header = deserializeWebHeader(decodedHeader);
    final OJwtPayload deserializeWebPayload = deserializeWebPayload(header.getType(), decodedPayload);
    token = new JsonWebToken(header, deserializeWebPayload);

    token.setIsVerified(verifyTokenSignature(header, tokenBytes, 0, secondDot, decodedSignature));
    return token;
  }

  @Override
  public boolean validateToken(final OToken token, final String command, final String database) {
    boolean valid = false;
    if (!(token instanceof JsonWebToken)) {
      return false;
    }
    final OrientJwtPayload payload = (OrientJwtPayload) ((JsonWebToken) token).getPayload();
    if (token.getDatabase().equalsIgnoreCase(database) && token.isNowValid()) {
      valid = true;
    }
    token.setIsValid(valid);
    return valid;
  }

  @Override
  public boolean validateBinaryToken(final OToken token) {
    boolean valid = false;
    if (token instanceof OBinaryToken && "node".equals(((OBinaryToken) token).getHeader().getType())) {
      valid = true;
    } else {
      final long curTime = System.currentTimeMillis();
      if (token.getExpiry() > curTime && (token.getExpiry() - (sessionInMills + 1)) < curTime) {
        valid = true;
      }
    }
    token.setIsValid(valid);

    return valid;
  }

  public byte[] getSignedWebToken(final ODatabaseDocument db, final OSecurityUser user) {
    final ByteArrayOutputStream tokenByteOS = new ByteArrayOutputStream(1024);
    final OrientJwtHeader header = new OrientJwtHeader();
    header.setAlgorithm("HS256");
    header.setKeyId("");

    final OJwtPayload payload = createPayload(db, user);
    header.setType(getPayloadType(payload));
    try {
      byte[] bytes = serializeWebHeader(header);
      tokenByteOS.write(Base64.getUrlEncoder().encode(ByteBuffer.wrap(bytes, 0, bytes.length)).array());
      tokenByteOS.write(JWT_DELIMITER);
      bytes = serializeWebPayload(payload);
      tokenByteOS.write(Base64.getUrlEncoder().encode(ByteBuffer.wrap(bytes, 0, bytes.length)).array());
      byte[] unsignedToken = tokenByteOS.toByteArray();
      tokenByteOS.write(JWT_DELIMITER);

      bytes = signToken(header, unsignedToken);
      tokenByteOS.write(Base64.getUrlEncoder().encode(ByteBuffer.wrap(bytes, 0, bytes.length)).array());
    } catch (Exception ex) {
      throw OException.wrapException(new OSystemException("Error on token parsing"), ex);
    }

    return tokenByteOS.toByteArray();
  }

  @Override
  public byte[] getDistributedToken(ONetworkProtocolData data) {
    try {

      final OBinaryToken token = new OBinaryToken();
      final OrientJwtHeader header = new OrientJwtHeader();
      header.setAlgorithm(algorithm);
      header.setKeyId(keyProvider.getDefaultKey());
      header.setType("node");
      token.setHeader(header);
      token.setServerUser(true);
      token.setUserName(data.serverUsername);
      token.setExpiry(0);
      token.setProtocolVersion(data.protocolVersion);
      token.setSerializer(data.getSerializationImpl());
      token.setDriverName(data.driverName);
      token.setDriverVersion(data.driverVersion);

      return serializeSignedToken(token);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw OException.wrapException(new OSystemException("Error on token parsing"), e);
    }
  }

  public byte[] getSignedBinaryToken(final ODatabaseDocumentInternal db, final OSecurityUser user,
      final ONetworkProtocolData data) {
    try {

      final OBinaryToken token = new OBinaryToken();

      long curTime = System.currentTimeMillis();

      final OrientJwtHeader header = new OrientJwtHeader();
      header.setAlgorithm(algorithm);
      header.setKeyId(keyProvider.getDefaultKey());
      header.setType("OrientDB");
      token.setHeader(header);
      if (db != null) {
        token.setDatabase(db.getName());
        token.setDatabaseType(db.getStorage().getUnderlying().getType());
      }
      if (data.serverUser) {
        token.setServerUser(true);
        token.setUserName(data.serverUsername);
      }
      if (user != null)
        token.setUserRid(user.getIdentity().getIdentity());
      token.setExpiry(curTime + sessionInMills);
      token.setProtocolVersion(data.protocolVersion);
      token.setSerializer(data.getSerializationImpl());
      token.setDriverName(data.driverName);
      token.setDriverVersion(data.driverVersion);

      return serializeSignedToken(token);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw OException.wrapException(new OSystemException("Error on token parsing"), e);
    }
  }

  private byte[] serializeSignedToken(OBinaryToken token) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    binarySerializer.serialize(token, baos);

    final byte[] signature = signToken(token.getHeader(), baos.toByteArray());
    baos.write(signature);

    return baos.toByteArray();
  }

  public ONetworkProtocolData getProtocolDataFromToken(OClientConnection connection, final OToken token) {
    if (token instanceof OBinaryToken) {
      final OBinaryToken binary = (OBinaryToken) token;
      final ONetworkProtocolData data = new ONetworkProtocolData();
      // data.clientId = binary.get;
      data.protocolVersion = binary.getProtocolVersion();
      data.setSerializationImpl(binary.getSerializer());
      data.driverName = binary.getDriverName();
      data.driverVersion = binary.getDriverVersion();
      data.serverUser = binary.isServerUser();
      data.serverUsername = binary.getUserName();
      data.serverUsername = binary.getUserName();
      data.supportsLegacyPushMessages = connection.getData().supportsLegacyPushMessages;
      data.collectStats = connection.getData().collectStats;
      return data;
    }
    return null;
  }

  @Override
  public OToken parseNotVerifyBinaryToken(byte[] binaryToken) {
    final ByteArrayInputStream bais = new ByteArrayInputStream(binaryToken);
    return deserializeBinaryToken(bais);
  }

  public OToken parseBinaryToken(final byte[] binaryToken) {
    try {
      final ByteArrayInputStream bais = new ByteArrayInputStream(binaryToken);

      final OBinaryToken token = deserializeBinaryToken(bais);
      final int end = binaryToken.length - bais.available();
      final byte[] decodedSignature = new byte[bais.available()];
      bais.read(decodedSignature);

      token.setIsVerified(verifyTokenSignature(token.getHeader(), binaryToken, 0, end, decodedSignature));
      return token;
    } catch (Exception e) {
      throw OException.wrapException(new OSystemException("Error on token parsing"), e);
    }
  }

  @Override
  public byte[] renewIfNeeded(final OToken token) {
    if (token == null)
      throw new IllegalArgumentException("Token is null");

    final long curTime = System.currentTimeMillis();
    if (token.getExpiry() - curTime < (sessionInMills / 2) && token.getExpiry() >= curTime) {
      final long expiryMinutes = sessionInMills;
      final long currTime = System.currentTimeMillis();
      token.setExpiry(currTime + expiryMinutes);
      try {
        if (token instanceof OBinaryToken)
          return serializeSignedToken((OBinaryToken) token);
        else
          throw new OTokenException("renew of web token not supported");
      } catch (IOException e) {
        throw OException.wrapException(new OSystemException("Error on token parsing"), e);
      }
    }
    return OCommonConst.EMPTY_BYTE_ARRAY;
  }

  public long getSessionInMills() {
    return sessionInMills;
  }

  public boolean isEnabled() {
    return true;
  }

  protected OrientJwtHeader deserializeWebHeader(final byte[] decodedHeader) {
    final ODocument doc = new ODocument();
    try {
      doc.fromJSON(new String(decodedHeader, "UTF-8"));
    } catch (UnsupportedEncodingException e) {
      throw OException.wrapException(new OSystemException("Header is not encoded in UTF-8 format"), e);
    }
    final OrientJwtHeader header = new OrientJwtHeader();
    header.setType((String) doc.field("typ"));
    header.setAlgorithm((String) doc.field("alg"));
    header.setKeyId((String) doc.field("kid"));
    return header;
  }

  protected OJwtPayload deserializeWebPayload(final String type, final byte[] decodedPayload) {
    if (!"OrientDB".equals(type)) {
      throw new OSystemException("Payload class not registered:" + type);
    }
    final ODocument doc = new ODocument();
    try {
      doc.fromJSON(new String(decodedPayload, "UTF-8"));
    } catch (UnsupportedEncodingException e) {
      throw OException.wrapException(new OSystemException("Payload encoding format differs from UTF-8"), e);
    }
    final OrientJwtPayload payload = new OrientJwtPayload();
    payload.setIssuer((String) doc.field("iss"));
    payload.setExpiry((Long) doc.field("exp"));
    payload.setIssuedAt((Long) doc.field("iat"));
    payload.setNotBefore((Long) doc.field("nbf"));
    payload.setDatabase((String) doc.field("sub"));
    payload.setAudience((String) doc.field("aud"));
    payload.setTokenId((String) doc.field("jti"));
    final int cluster = (Integer) doc.field("uidc");
    final long pos = (Long) doc.field("uidp");
    payload.setUserRid(new ORecordId(cluster, pos));
    payload.setDatabaseType((String) doc.field("bdtyp"));
    return payload;
  }

  protected byte[] serializeWebHeader(final OJwtHeader header) throws Exception {
    if (header == null)
      throw new IllegalArgumentException("Token header is null");

    ODocument doc = new ODocument();
    doc.field("typ", header.getType());
    doc.field("alg", header.getAlgorithm());
    doc.field("kid", header.getKeyId());
    return doc.toJSON().getBytes("UTF-8");
  }

  protected byte[] serializeWebPayload(final OJwtPayload payload) throws Exception {
    if (payload == null)
      throw new IllegalArgumentException("Token payload is null");

    final ODocument doc = new ODocument();
    doc.field("iss", payload.getIssuer());
    doc.field("exp", payload.getExpiry());
    doc.field("iat", payload.getIssuedAt());
    doc.field("nbf", payload.getNotBefore());
    doc.field("sub", payload.getDatabase());
    doc.field("aud", payload.getAudience());
    doc.field("jti", payload.getTokenId());
    doc.field("uidc", ((OrientJwtPayload) payload).getUserRid().getClusterId());
    doc.field("uidp", ((OrientJwtPayload) payload).getUserRid().getClusterPosition());
    doc.field("bdtyp", ((OrientJwtPayload) payload).getDatabaseType());
    return doc.toJSON().getBytes("UTF-8");
  }

  protected OJwtPayload createPayload(final ODatabaseDocument db, final OSecurityUser user) {
    if (user == null)
      throw new IllegalArgumentException("User is null");

    final OrientJwtPayload payload = new OrientJwtPayload();
    payload.setAudience("OrientDB");
    payload.setDatabase(db.getName());
    payload.setUserRid(user.getDocument().getIdentity());

    final long expiryMinutes = sessionInMills;
    final long currTime = System.currentTimeMillis();
    payload.setIssuedAt(currTime);
    payload.setNotBefore(currTime);
    payload.setUserName(user.getName());
    payload.setTokenId(UUID.randomUUID().toString());
    payload.setExpiry(currTime + expiryMinutes);
    return payload;
  }

  protected String getPayloadType(final OJwtPayload payload) {
    return "OrientDB";
  }

  protected OKeyProvider getKeyProvider() {
    return keyProvider;
  }

  private boolean verifyTokenSignature(final OJwtHeader header, final byte[] base, final int baseOffset, final int baseLength,
      final byte[] signature) {
    final Mac mac = threadLocalMac.get();

    try {
      mac.init(getKeyProvider().getKey(header));
      mac.update(base, baseOffset, baseLength);
      final byte[] calculatedSignature = mac.doFinal();
      boolean valid = MessageDigest.isEqual(calculatedSignature, signature);
      if (!valid) {
        OLogManager.instance().warn(this, "Token signature failure: %s", Base64.getEncoder().encodeToString(base));
      }
      return valid;

    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw OException.wrapException(new OSystemException("Token signature cannot be verified"), e);
    } finally {
      mac.reset();
    }
  }

  private byte[] signToken(final OJwtHeader header, final byte[] unsignedToken) {
    final Mac mac = threadLocalMac.get();
    try {
      mac.init(getKeyProvider().getKey(header));
      return mac.doFinal(unsignedToken);
    } catch (Exception ex) {
      throw OException.wrapException(new OSystemException("Error on token parsing"), ex);
    } finally {
      mac.reset();
    }
  }

  private OBinaryToken deserializeBinaryToken(final InputStream bais) {
    try {
      return binarySerializer.deserialize(bais);
    } catch (Exception e) {
      throw OException.wrapException(new OSystemException("Cannot deserialize binary token"), e);
    }
  }

  private static class MacThreadLocal extends ThreadLocal<Mac> {
    @Override
    protected Mac initialValue() {
      try {
        return Mac.getInstance(algorithm);
      } catch (NoSuchAlgorithmException nsa) {
        throw new IllegalArgumentException("Can't find encryption algorithm '" + algorithm + "'", nsa);
      }
    }
  }
}
