package com.orientechnologies.orient.server.jwt.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Date;
import java.util.UUID;

import javax.crypto.Mac;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.metadata.security.OSecurityUser;
import com.orientechnologies.orient.core.metadata.security.OToken;
import com.orientechnologies.orient.core.metadata.security.OTokenHandler;
import com.orientechnologies.orient.core.metadata.security.jwt.OJwtHeader;
import com.orientechnologies.orient.core.metadata.security.jwt.OJwtKeyProvider;
import com.orientechnologies.orient.core.metadata.security.jwt.OJwtPayload;
import com.orientechnologies.orient.core.serialization.OBase64Utils;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.jwt.mixin.OJwtHeaderMixin;
import com.orientechnologies.orient.server.jwt.mixin.OJwtPayloadMixin;
import com.orientechnologies.orient.server.plugin.OServerPluginAbstract;

/**
 * Created by emrul on 27/10/2014.
 *
 * @author Emrul Islam <emrul@emrul.com> Copyright 2014 Emrul Islam
 */
public class JwtTokenHandler extends OServerPluginAbstract implements OTokenHandler {
  public static final String            O_SIGN_KEY        = "oAuth2Key";

  private static final String           JWT_TOKEN_HANDLER = "JwtTokenHandler";

  private final ObjectMapper            mapper;

  protected static final int            JWT_DELIMITER     = '.';

  private static final ThreadLocal<Mac> threadLocalMac    = new ThreadLocal<Mac>() {
                                                            @Override
                                                            protected Mac initialValue() {
                                                              try {
                                                                return Mac.getInstance("HmacSHA256");
                                                              } catch (NoSuchAlgorithmException nsa) {
                                                                throw new IllegalArgumentException("Can't find algorithm.");
                                                              }
                                                            }
                                                          };

  private OJwtKeyProvider               keyProvider;

  public JwtTokenHandler() {
    mapper = new ObjectMapper().registerModule(new AfterburnerModule()).configure(
        DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    mapper.addMixInAnnotations(OJwtHeader.class, OJwtHeaderMixin.class);
    mapper.addMixInAnnotations(OJwtPayload.class, OJwtPayloadMixin.class);

  }

  @Override
  public void config(final OServer iServer, final OServerParameterConfiguration[] iParams) {

    for (OServerParameterConfiguration param : iParams) {
      if (param.name.equalsIgnoreCase(O_SIGN_KEY)) {
        byte secret[] = OBase64Utils.decode(param.value, OBase64Utils.URL_SAFE);
        keyProvider = new DefaultJwtKeyProvider(secret);
      }
    }

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
    if (payload.getDbName().equalsIgnoreCase(database) && payload.getExpiry() > System.currentTimeMillis()
        && payload.getNotBefore() < System.currentTimeMillis()) {
      valid = true;
    }
    // TODO: Other validations... (e.g. check audience, etc.)
    token.setIsValid(valid);
    return valid;
  }

  protected OrientJwtHeader deserializeWebHeader(byte[] decodedHeader) {
    try {
      return mapper.readValue(decodedHeader, OrientJwtHeader.class);
    } catch (Exception e) {
      throw new OException(e);
    }
  }

  protected OJwtPayload deserializeWebPayload(String type, byte[] decodedPayload) {
    if (!"OrientDB".equals(type)) {
      throw new OException("Payload class not registered:" + type);
    }
    try {
      return mapper.readValue(decodedPayload, OrientJwtPayload.class);
    } catch (Exception e) {
      throw new OException(e);
    }
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
    return mapper.writeValueAsBytes(header);
  }

  protected byte[] serializeWebPayload(OJwtPayload payload) throws Exception {
    return mapper.writeValueAsBytes(payload);
  }

  protected OJwtPayload createPayload(ODatabaseDocumentInternal db, OSecurityUser user) {
    OrientJwtPayload payload = new OrientJwtPayload();
    payload.setAudience("OrientDb");
    payload.setDbName(db.getName());
    payload.setUserRid(user.getDocument().getIdentity().toString());

    long expiryMinutes = 60000 * 10;
    long currTime = System.currentTimeMillis();
    Date issueTime = new Date(currTime);
    Date expDate = new Date(currTime + expiryMinutes);
    payload.setIssuedAt(issueTime.getTime());
    payload.setNotBefore(issueTime.getTime());
    payload.setSubject(user.getName());
    payload.setTokenId(UUID.randomUUID().toString());
    payload.setExpiry(expDate.getTime());
    return payload;
  }

  public byte[] getSignedBinaryToken(ODatabaseDocumentInternal db, OSecurityUser user) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      OrientJwtHeader header = new OrientJwtHeader();
      header.setAlgorithm("HS256");
      header.setKeyId("");
      serializeBinaryHeader(header, baos);
      OJwtPayload payload = createPayload(db, user);
      serializeBinaryPayload(payload, baos);

      byte[] signature = signToken(header, baos.toByteArray());
      baos.write(signature);

    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new OException(e);
    }
    return baos.toByteArray();
  }

  private void serializeBinaryPayload(OJwtPayload payload, OutputStream baos) throws Exception {
    byte[] res = serializeWebPayload(payload);
    baos.write(res.length);
    baos.write(res);
  }

  private void serializeBinaryHeader(OrientJwtHeader header, OutputStream baos) throws Exception {
    byte[] res = serializeWebHeader(header);
    baos.write(res.length);
    baos.write(res);
  }

  public OToken parseBinaryToken(byte[] binaryToken) {
    try {
      ByteArrayInputStream bais = new ByteArrayInputStream(binaryToken);

      OJwtHeader header = deserializeBinaryHeader(bais);
      OJwtPayload payload = deserializeBinaryPayload(bais);

      JsonWebToken token = new JsonWebToken(header, payload);
      int end = binaryToken.length - bais.available();
      byte[] decodedSignature = new byte[bais.available()];
      bais.read(decodedSignature);

      token.setIsVerified(verifyTokenSignature(header, binaryToken, 0, end, decodedSignature));
      return token;
    } catch (IOException e) {
      throw new OException(e);
    }
  }

  private OJwtPayload deserializeBinaryPayload(InputStream bais) {
    try {
      int size = bais.read();
      byte[] data = new byte[size];
      bais.read(data);
      return mapper.readValue(data, OrientJwtPayload.class);
    } catch (Exception e) {
      throw new OException(e);
    }
  }

  private OJwtHeader deserializeBinaryHeader(InputStream bais) {
    try {
      int size = bais.read();
      byte[] data = new byte[size];
      bais.read(data);
      return mapper.readValue(data, OrientJwtHeader.class);
    } catch (Exception e) {
      throw new OException(e);
    }
  }

  protected String getPayloadType(OJwtPayload payload) {
    return "OrientDB";
  }

  @Override
  public String getName() {
    return JWT_TOKEN_HANDLER;
  }

  protected OJwtKeyProvider getKeyProvider() {
    return keyProvider;
  }

}
