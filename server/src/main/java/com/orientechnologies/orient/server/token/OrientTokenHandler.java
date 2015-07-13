package com.orientechnologies.orient.server.token;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.UUID;

import javax.crypto.Mac;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.util.OCommonConst;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.security.OSecurityUser;
import com.orientechnologies.orient.core.metadata.security.OToken;
import com.orientechnologies.orient.core.metadata.security.jwt.OJwtHeader;
import com.orientechnologies.orient.core.metadata.security.jwt.OJwtPayload;
import com.orientechnologies.orient.core.metadata.security.jwt.OKeyProvider;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.OBase64Utils;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OTokenHandler;
import com.orientechnologies.orient.server.binary.impl.OBinaryToken;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.network.protocol.ONetworkProtocolData;
import com.orientechnologies.orient.server.plugin.OServerPluginAbstract;

/**
 * Created by emrul on 27/10/2014.
 *
 * @author Emrul Islam <emrul@emrul.com> Copyright 2014 Emrul Islam
 */
public class OrientTokenHandler extends OServerPluginAbstract implements OTokenHandler {
  public static final String            SIGN_KEY_PAR                 = "oAuth2Key";
  public static final String            SESSION_LENGHT_PAR           = "sessionLength";
  public static final String            ENCRYPTION_ALGORITHM_PAR     = "encryptionAlgorithm";
  public static final String            ENCRYPTION_ALGORITHM_DEFAULT = "HmacSHA256";

  private static String                 algorithm                    = ENCRYPTION_ALGORITHM_DEFAULT;
  private static final ThreadLocal<Mac> threadLocalMac               = new MacThreadLocal();

  protected static final int            JWT_DELIMITER                = '.';
  private boolean                       enabled                      = false;
  private OBinaryTokenSerializer        binarySerializer;
  private long                          sessionInMills               = 1000 * 60 * 60;              // 1 HOUR
  private OKeyProvider                  keyProvider;

  @Override
  public void config(final OServer iServer, final OServerParameterConfiguration[] iParams) {

    for (OServerParameterConfiguration param : iParams) {
      if (param.name.equalsIgnoreCase("enabled")) {
        if (Boolean.parseBoolean(param.value))
          // ENABLE IT
          enabled = true;
      } else if (param.name.equalsIgnoreCase(SIGN_KEY_PAR)) {
        byte secret[] = OBase64Utils.decode(param.value, OBase64Utils.URL_SAFE);
        keyProvider = new DefaultKeyProvider(secret);
      } else if (param.name.equalsIgnoreCase(SESSION_LENGHT_PAR)) {
        sessionInMills = Long.parseLong(param.value) * 1000 * 60;
      } else if (param.name.equalsIgnoreCase(ENCRYPTION_ALGORITHM_PAR)) {
        algorithm = param.value;
        try {
          Mac.getInstance(algorithm);
        } catch (NoSuchAlgorithmException nsa) {
          throw new IllegalArgumentException("Can't find encryption algorithm '" + algorithm + "'");
        }
      }

    }
    String[] keys = keyProvider.getKeys();
    this.binarySerializer = new OBinaryTokenSerializer(new String[] { "plocal", "memory" }, keys, new String[] { algorithm },
        new String[] { "OrientDB" });
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

    final byte[] decodedHeader = OBase64Utils.decode(tokenBytes, 0, firstDot, OBase64Utils.URL_SAFE);
    final byte[] decodedPayload = OBase64Utils.decode(tokenBytes, firstDot + 1, secondDot - (firstDot + 1), OBase64Utils.URL_SAFE);
    final byte[] decodedSignature = OBase64Utils.decode(tokenBytes, secondDot + 1, tokenBytes.length - (secondDot + 1),
        OBase64Utils.URL_SAFE);

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
    if (token.getDatabase().equalsIgnoreCase(database) && token.getExpiry() > System.currentTimeMillis()
        && payload.getNotBefore() < System.currentTimeMillis()) {
      valid = true;
    }
    // TODO: Other validations... (e.g. check audience, etc.)
    token.setIsValid(valid);
    return valid;
  }

  @Override
  public boolean validateBinaryToken(final OToken token) {
    boolean valid = false;
    final long curTime = System.currentTimeMillis();
    if (token.getExpiry() > curTime && (token.getExpiry() - (sessionInMills + 1)) < curTime) {
      valid = true;
    }
    // TODO: Other validations... (e.g. check audience, etc.)
    token.setIsValid(valid);
    return valid;
  }

  public byte[] getSignedWebToken(final ODatabaseDocumentInternal db, final OSecurityUser user) {
    final ByteArrayOutputStream tokenByteOS = new ByteArrayOutputStream(1024);
    final OrientJwtHeader header = new OrientJwtHeader();
    header.setAlgorithm("HS256");
    header.setKeyId("");

    final OJwtPayload payload = createPayload(db, user);
    header.setType(getPayloadType(payload));
    try {
      byte[] bytes = serializeWebHeader(header);
      tokenByteOS.write(OBase64Utils.encodeBytesToBytes(bytes, 0, bytes.length, OBase64Utils.URL_SAFE));
      tokenByteOS.write(JWT_DELIMITER);
      bytes = serializeWebPayload(payload);
      tokenByteOS.write(OBase64Utils.encodeBytesToBytes(bytes, 0, bytes.length, OBase64Utils.URL_SAFE));
      byte[] unsignedToken = tokenByteOS.toByteArray();
      tokenByteOS.write(JWT_DELIMITER);

      bytes = signToken(header, unsignedToken);
      tokenByteOS.write(OBase64Utils.encodeBytesToBytes(bytes, 0, bytes.length, OBase64Utils.URL_SAFE));
    } catch (Exception ex) {
      throw new OException("Error on token parsing", ex);
    }

    return tokenByteOS.toByteArray();
  }

  public byte[] getSignedBinaryToken(final ODatabaseDocumentInternal db, final OSecurityUser user, final ONetworkProtocolData data) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      final OBinaryToken token = new OBinaryToken();

      long expiryMinutes = sessionInMills;
      long currTime = System.currentTimeMillis();

      final OrientJwtHeader header = new OrientJwtHeader();
      header.setAlgorithm(algorithm);
      header.setKeyId(algorithm);
      header.setType("OrientDB");
      token.setHeader(header);
      if (db != null) {
        token.setDatabase(db.getName());
        token.setDatabaseType(db.getStorage().getType());
      }
      if (data.serverUser) {
        token.setServerUser(true);
        token.setUserName(data.serverUsername);
      }
      if (user != null)
        token.setUserRid(user.getIdentity().getIdentity());
      token.setExpiry(currTime + expiryMinutes);
      token.setProtocolVersion(data.protocolVersion);
      token.setSerializer(data.serializationImpl);
      token.setDriverName(data.driverName);
      token.setDriverVersion(data.driverVersion);

      binarySerializer.serialize(token, baos);

      final byte[] signature = signToken(header, baos.toByteArray());
      baos.write(signature);

    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new OException(e);
    }
    return baos.toByteArray();
  }

  public ONetworkProtocolData getProtocolDataFromToken(final OToken token) {
    if (token instanceof OBinaryToken) {
      final OBinaryToken binary = (OBinaryToken) token;
      final ONetworkProtocolData data = new ONetworkProtocolData();
      // data.clientId = binary.get;
      data.protocolVersion = binary.getProtocolVersion();
      data.serializationImpl = binary.getSerializer();
      data.driverName = binary.getDriverName();
      data.driverVersion = binary.getDriverVersion();
      data.serverUser = binary.isServerUser();
      data.serverUsername = binary.getUserName();
      return data;
    }
    return null;
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
    } catch (IOException e) {
      throw new OException(e);
    }
  }

  @Override
  public String getName() {
    return OTokenHandler.TOKEN_HANDLER_NAME;
  }

  @Override
  public byte[] renewIfNeeded(final OToken token) {
    if (token == null)
      throw new IllegalArgumentException("Token is null");

    final long curTime = System.currentTimeMillis();
    if (token.getExpiry() + (sessionInMills / 2) > curTime && (token.getExpiry() - (sessionInMills + 1)) < curTime) {
      final long expiryMinutes = sessionInMills;
      final long currTime = System.currentTimeMillis();
      token.setExpiry(currTime + expiryMinutes);
    }
    return OCommonConst.EMPTY_BYTE_ARRAY;
  }

  public long getSessionInMills() {
    return sessionInMills;
  }

  public boolean isEnabled() {
    return enabled;
  }

  protected OrientJwtHeader deserializeWebHeader(final byte[] decodedHeader) {
    final ODocument doc = new ODocument();
    try {
      doc.fromJSON(new String(decodedHeader, "UTF-8"));
    } catch (UnsupportedEncodingException e) {
      throw new OException(e);
    }
    final OrientJwtHeader header = new OrientJwtHeader();
    header.setType((String) doc.field("typ"));
    header.setAlgorithm((String) doc.field("alg"));
    header.setKeyId((String) doc.field("kid"));
    return header;
  }

  protected OJwtPayload deserializeWebPayload(final String type, final byte[] decodedPayload) {
    if (!"OrientDB".equals(type)) {
      throw new OException("Payload class not registered:" + type);
    }
    final ODocument doc = new ODocument();
    try {
      doc.fromJSON(new String(decodedPayload, "UTF-8"));
    } catch (UnsupportedEncodingException e) {
      throw new OException(e);
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

  protected OJwtPayload createPayload(final ODatabaseDocumentInternal db, final OSecurityUser user) {
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

      return Arrays.equals(calculatedSignature, signature);

    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new OException(e);
    } finally {
      mac.reset();
    }
  }

  private byte[] signToken(final OrientJwtHeader header, final byte[] unsignedToken) {
    final Mac mac = threadLocalMac.get();
    try {
      mac.init(getKeyProvider().getKey(header));
      return mac.doFinal(unsignedToken);
    } catch (Exception ex) {
      throw new OException("Error on token parsing", ex);
    } finally {
      mac.reset();
    }
  }

  private OBinaryToken deserializeBinaryToken(final InputStream bais) {
    try {
      return binarySerializer.deserialize(bais);
    } catch (Exception e) {
      throw new OException(e);
    }
  }

  private static class MacThreadLocal extends ThreadLocal<Mac> {
    @Override
    protected Mac initialValue() {
      try {
        return Mac.getInstance(algorithm);
      } catch (NoSuchAlgorithmException nsa) {
        throw new IllegalArgumentException("Can't find encryption algorithm '" + algorithm + "'");
      }
    }
  }
}
