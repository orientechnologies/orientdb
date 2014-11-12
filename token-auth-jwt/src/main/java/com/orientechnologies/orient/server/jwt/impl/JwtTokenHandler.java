package com.orientechnologies.orient.server.jwt.impl;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Date;
import java.util.UUID;

import javax.crypto.Mac;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.exception.OSecurityAccessException;
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

  private static final int              JWT_DELIMITER     = '.';

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
  public OToken parseWebToken(byte[] tokenBytes) throws InvalidKeyException, NoSuchAlgorithmException, IOException {
    OToken token = null;

    // / <header>.<payload>.<signature>
    int firstDot = -1, secondDot = -1;
    int x;
    for (x = 0; x < tokenBytes.length; x++) {
      if (tokenBytes[x] == JWT_DELIMITER) {
        firstDot = x; // stores reference to first '.' character in JWT token
        break;
      }
    }
    if (firstDot == -1)
      return null;

    for (x = firstDot + 1; x < tokenBytes.length; x++) {
      if (tokenBytes[x] == JWT_DELIMITER) {
        secondDot = x; // stores reference to second '.' character in JWT token
        break;
      }
    }
    if (secondDot == -1)
      return null;

    byte[] decodedHeader = OBase64Utils.decode(tokenBytes, 0, firstDot, OBase64Utils.URL_SAFE);
    JwtHeader header = deserializeWebHeader(decodedHeader);

    Mac mac = threadLocalMac.get();

    try {
      mac.init(getKeyProvider().getKey(header));
      mac.update(tokenBytes, 0, secondDot);
      byte[] calculatedSignature = mac.doFinal();

      byte[] decodedSignature = OBase64Utils.decode(tokenBytes, secondDot + 1, tokenBytes.length - (secondDot + 1),
          OBase64Utils.URL_SAFE);

      boolean signatureValid = Arrays.equals(calculatedSignature, decodedSignature);

      if (signatureValid) {
        byte[] decodedPayload = OBase64Utils.decode(tokenBytes, firstDot + 1, secondDot - (firstDot + 1), OBase64Utils.URL_SAFE);
        token = new JsonWebToken(header, deserializeWebPayload(header.getType(), decodedPayload));
        token.setIsVerified(true);
        return token;
      }

    } catch (Exception ex) {
      OLogManager.instance().warn(this, "Error parsing token", ex);
      // noop
    } finally {
      mac.reset();
    }
    return token;
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

  protected JwtHeader deserializeWebHeader(byte[] decodedHeader) throws JsonParseException, JsonMappingException, IOException {
    return mapper.readValue(decodedHeader, JwtHeader.class);
  }

  protected OJwtPayload deserializeWebPayload(String type, byte[] decodedPayload) throws Exception {
    if (!"OrientDB".equals(type)) {
      throw new Exception("Payload class not registered:" + type);
    }
    return mapper.readValue(decodedPayload, OrientJwtPayload.class);
  }

  public byte[] getSignedWebToken(ODatabaseDocumentInternal db, OSecurityUser user) {
    ByteArrayOutputStream tokenByteOS = new ByteArrayOutputStream(1024);
    JwtHeader header = new JwtHeader();
    header.setAlgorithm("HS256");
    header.setKeyId("");

    OJwtPayload payload = createPayload(db, user);
    header.setType(getPayloadType(payload));

    Mac mac = threadLocalMac.get();

    try {
      byte[] bytes = serializeHeader(header);
      tokenByteOS.write(OBase64Utils.encodeBytesToBytes(bytes, 0, bytes.length, OBase64Utils.URL_SAFE));
      tokenByteOS.write(JWT_DELIMITER);
      bytes = serializePayload(payload);
      tokenByteOS.write(OBase64Utils.encodeBytesToBytes(bytes, 0, bytes.length, OBase64Utils.URL_SAFE));

      byte[] unsignedToken = tokenByteOS.toByteArray();

      tokenByteOS.write(JWT_DELIMITER);
      mac.init(getKeyProvider().getKey(header));
      bytes = mac.doFinal(unsignedToken);
      tokenByteOS.write(OBase64Utils.encodeBytesToBytes(bytes, 0, bytes.length, OBase64Utils.URL_SAFE));

    } catch (Exception ex) {
      OLogManager.instance().error(this, "Error signing token", ex);
      throw new OSecurityAccessException(db.getName(), "Error on token parsion", ex);
    }

    return tokenByteOS.toByteArray();
  }

  protected byte[] serializeHeader(OJwtHeader header) throws JsonProcessingException {
    return mapper.writeValueAsBytes(header);
  }

  protected byte[] serializePayload(OJwtPayload payload) throws JsonProcessingException {
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
