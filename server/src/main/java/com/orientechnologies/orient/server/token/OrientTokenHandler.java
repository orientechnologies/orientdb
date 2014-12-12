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
  public static final String            O_SIGN_KEY       = "oAuth2Key";
  public static final String            O_SESSION_LENGHT = "sessionLength";

  private OBinaryTokenSerializer        binarySerializer;
  protected boolean                     enabled          = false;

  protected static final int            JWT_DELIMITER    = '.';

  private int                           sessionInMills   = 1000 * 60 * 60;

  private static final ThreadLocal<Mac> threadLocalMac   = new ThreadLocal<Mac>() {
                                                           @Override
                                                           protected Mac initialValue() {
                                                             try {
                                                               return Mac.getInstance("HmacSHA256");
                                                             } catch (NoSuchAlgorithmException nsa) {
                                                               throw new IllegalArgumentException("Can't find algorithm.");
                                                             }
                                                           }
                                                         };

  private OKeyProvider                  keyProvider;

  @Override
  public void config(final OServer iServer, final OServerParameterConfiguration[] iParams) {

    for (OServerParameterConfiguration param : iParams) {
      if (param.name.equalsIgnoreCase("enabled")) {
        if (Boolean.parseBoolean(param.value))
          // ENABLE IT
          enabled = true;
      } else if (param.name.equalsIgnoreCase(O_SIGN_KEY)) {
        byte secret[] = OBase64Utils.decode(param.value, OBase64Utils.URL_SAFE);
        keyProvider = new DefaultKeyProvider(secret);
      } else if (param.name.equalsIgnoreCase(O_SESSION_LENGHT)) {
        sessionInMills = Integer.parseInt(param.value) * 3600;
      }

    }
    String[] keys = keyProvider.getKeys();
    this.binarySerializer = new OBinaryTokenSerializer(new String[] { "plocal", "memory" }, keys, new String[] { "HmacSHA256" },
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
      throw new RuntimeException("Token data too short missed header");

    if (secondDot == -1)
      throw new RuntimeException("Token data too short missed signature");

    byte[] decodedHeader = OBase64Utils.decode(tokenBytes, 0, firstDot, OBase64Utils.URL_SAFE);
    byte[] decodedPayload = OBase64Utils.decode(tokenBytes, firstDot + 1, secondDot - (firstDot + 1), OBase64Utils.URL_SAFE);
    byte[] decodedSignature = OBase64Utils.decode(tokenBytes, secondDot + 1, tokenBytes.length - (secondDot + 1),
        OBase64Utils.URL_SAFE);

    OrientJwtHeader header = deserializeWebHeader(decodedHeader);
    OJwtPayload deserializeWebPayload = deserializeWebPayload(header.getType(), decodedPayload);
    token = new JsonWebToken(header, deserializeWebPayload);

    token.setIsVerified(verifyTokenSignature(header, tokenBytes, 0, secondDot, decodedSignature));
    return token;
  }

  private boolean verifyTokenSignature(OJwtHeader header, byte[] base, int baseOffset, int baseLength, byte[] signature) {
    Mac mac = threadLocalMac.get();

    try {
      mac.init(getKeyProvider().getKey(header));
      mac.update(base, baseOffset, baseLength);
      byte[] calculatedSignature = mac.doFinal();

      return Arrays.equals(calculatedSignature, signature);

    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new OException(e);
    } finally {
      mac.reset();
    }
  }

  @Override
  public boolean validateToken(OToken token, String command, String database) {
    boolean valid = false;
    if (!(token instanceof JsonWebToken)) {
      return false;
    }
    OrientJwtPayload payload = (OrientJwtPayload) ((JsonWebToken) token).getPayload();
    if (token.getDatabase().equalsIgnoreCase(database) && token.getExpiry() > System.currentTimeMillis()
        && payload.getNotBefore() < System.currentTimeMillis()) {
      valid = true;
    }
    // TODO: Other validations... (e.g. check audience, etc.)
    token.setIsValid(valid);
    return valid;
  }

  @Override
  public boolean validateBinaryToken(OToken token) {
    boolean valid = false;
    long curTime = System.currentTimeMillis();
    if (token.getExpiry() > curTime && (token.getExpiry() - (sessionInMills + 1)) < curTime) {
      valid = true;
    }
    // TODO: Other validations... (e.g. check audience, etc.)
    token.setIsValid(valid);
    return valid;
  }

  protected OrientJwtHeader deserializeWebHeader(byte[] decodedHeader) {
    ODocument doc = new ODocument();
    try {
      doc.fromJSON(new String(decodedHeader, "UTF-8"));
    } catch (UnsupportedEncodingException e) {
      throw new OException(e);
    }
    OrientJwtHeader header = new OrientJwtHeader();
    header.setType((String) doc.field("typ"));
    header.setAlgorithm((String) doc.field("alg"));
    header.setKeyId((String) doc.field("kid"));
    return header;
  }

  protected OJwtPayload deserializeWebPayload(String type, byte[] decodedPayload) {
    if (!"OrientDB".equals(type)) {
      throw new OException("Payload class not registered:" + type);
    }
    ODocument doc = new ODocument();
    try {
      doc.fromJSON(new String(decodedPayload, "UTF-8"));
    } catch (UnsupportedEncodingException e) {
      throw new OException(e);
    }
    OrientJwtPayload payload = new OrientJwtPayload();
    payload.setIssuer((String) doc.field("iss"));
    payload.setExpiry((Long) doc.field("exp"));
    payload.setIssuedAt((Long) doc.field("iat"));
    payload.setNotBefore((Long) doc.field("nbf"));
    payload.setDatabase((String) doc.field("sub"));
    payload.setAudience((String) doc.field("aud"));
    payload.setTokenId((String) doc.field("jti"));
    int cluster = (Integer) doc.field("uidc");
    long pos = (Long) doc.field("uidp");
    payload.setUserRid(new ORecordId(cluster, pos));
    payload.setDatabaseType((String) doc.field("bdtyp"));
    return payload;
  }

  public byte[] getSignedWebToken(ODatabaseDocumentInternal db, OSecurityUser user) {
    ByteArrayOutputStream tokenByteOS = new ByteArrayOutputStream(1024);
    OrientJwtHeader header = new OrientJwtHeader();
    header.setAlgorithm("HS256");
    header.setKeyId("");

    OJwtPayload payload = createPayload(db, user);
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

  private byte[] signToken(OrientJwtHeader header, byte[] unsignedToken) {
    Mac mac = threadLocalMac.get();
    try {
      mac.init(getKeyProvider().getKey(header));
      return mac.doFinal(unsignedToken);
    } catch (Exception ex) {
      throw new OException("Error on token parsing", ex);
    } finally {
      mac.reset();
    }
  }

  protected byte[] serializeWebHeader(OJwtHeader header) throws Exception {
    ODocument doc = new ODocument();
    doc.field("typ", header.getType());
    doc.field("alg", header.getAlgorithm());
    doc.field("kid", header.getKeyId());
    return doc.toJSON().getBytes("UTF-8");
  }

  protected byte[] serializeWebPayload(OJwtPayload payload) throws Exception {
    ODocument doc = new ODocument();
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

  protected OJwtPayload createPayload(ODatabaseDocumentInternal db, OSecurityUser user) {
    OrientJwtPayload payload = new OrientJwtPayload();
    payload.setAudience("OrientDb");
    payload.setDatabase(db.getName());
    payload.setUserRid(user.getDocument().getIdentity());

    long expiryMinutes = sessionInMills;
    long currTime = System.currentTimeMillis();
    payload.setIssuedAt(currTime);
    payload.setNotBefore(currTime);
    payload.setUserName(user.getName());
    payload.setTokenId(UUID.randomUUID().toString());
    payload.setExpiry(currTime + expiryMinutes);
    return payload;
  }

  public byte[] getSignedBinaryToken(ODatabaseDocumentInternal db, OSecurityUser user, ONetworkProtocolData data) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      OBinaryToken token = new OBinaryToken();

      long expiryMinutes = sessionInMills;
      long currTime = System.currentTimeMillis();

      OrientJwtHeader header = new OrientJwtHeader();
      header.setAlgorithm("HmacSHA256");
      header.setKeyId("HmacSHA256");
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

      byte[] signature = signToken(header, baos.toByteArray());
      baos.write(signature);

    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new OException(e);
    }
    return baos.toByteArray();
  }

  public ONetworkProtocolData getProtocolDataFromToken(OToken token) {
    if (token instanceof OBinaryToken) {
      OBinaryToken binary = (OBinaryToken) token;
      ONetworkProtocolData data = new ONetworkProtocolData();
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

  public OToken parseBinaryToken(byte[] binaryToken) {
    try {
      ByteArrayInputStream bais = new ByteArrayInputStream(binaryToken);

      OBinaryToken token = deserializeBinaryToken(bais);
      int end = binaryToken.length - bais.available();
      byte[] decodedSignature = new byte[bais.available()];
      bais.read(decodedSignature);

      token.setIsVerified(verifyTokenSignature(token.getHeader(), binaryToken, 0, end, decodedSignature));
      return token;
    } catch (IOException e) {
      throw new OException(e);
    }
  }

  private OBinaryToken deserializeBinaryToken(InputStream bais) {
    try {
      return binarySerializer.deserialize(bais);
    } catch (Exception e) {
      throw new OException(e);
    }
  }

  protected String getPayloadType(OJwtPayload payload) {
    return "OrientDB";
  }

  @Override
  public String getName() {
    return OTokenHandler.TOKEN_HANDLER_NAME;
  }

  protected OKeyProvider getKeyProvider() {
    return keyProvider;
  }

  @Override
  public byte[] renewIfNeeded(OToken token) {
    long curTime = System.currentTimeMillis();
    if (token.getExpiry() + (sessionInMills / 2) > curTime && (token.getExpiry() - (sessionInMills + 1)) < curTime) {
      long expiryMinutes = sessionInMills;
      long currTime = System.currentTimeMillis();
      token.setExpiry(currTime + expiryMinutes);
    }
    return new byte[] {};
  }

  public int getSessionInMills() {
    return sessionInMills;
  }

  public boolean isEnabled() {
    return enabled;
  }

}
