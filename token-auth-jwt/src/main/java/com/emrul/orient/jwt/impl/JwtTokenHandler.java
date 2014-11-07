package com.emrul.orient.jwt.impl;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import javax.crypto.Mac;

import com.emrul.orient.jwt.mixin.IJwtHeaderMixin;
import com.emrul.orient.jwt.mixin.IJwtPayloadMixin;
import com.emrul.util.Base64;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.metadata.security.IToken;
import com.orientechnologies.orient.core.metadata.security.ITokenHandler;
import com.orientechnologies.orient.core.metadata.security.OSecurityUser;
import com.orientechnologies.orient.core.metadata.security.jwt.IJwtHeader;
import com.orientechnologies.orient.core.metadata.security.jwt.IJwtKeyProvider;
import com.orientechnologies.orient.core.metadata.security.jwt.IJwtPayload;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.plugin.OServerPluginAbstract;

/**
 * Created by emrul on 27/10/2014.
 *
 * @author Emrul Islam <emrul@emrul.com> Copyright 2014 Emrul Islam
 */
public class JwtTokenHandler extends OServerPluginAbstract implements ITokenHandler {
  private final ObjectMapper                       mapper;

  private static final int                         JWT_DELIMITER  = '.';

  public static final Base64.Encoder               b64encoder     = Base64.getUrlEncoder();
  public static final Base64.Decoder               b64decoder     = Base64.getUrlDecoder();

  protected final ConcurrentHashMap<String, Class> payloadClasses = new ConcurrentHashMap<String, Class>();

  private static final ThreadLocal<Mac>            threadLocalMac = new ThreadLocal<Mac>() {
                                                                    @Override
                                                                    protected Mac initialValue() {
                                                                      try {
                                                                        return Mac.getInstance("HmacSHA256");
                                                                      } catch (NoSuchAlgorithmException nsa) {
                                                                        throw new IllegalArgumentException("Can't find algorithm.");
                                                                      }
                                                                    }
                                                                  };

  private OServer                                  serverInstance;
  private IJwtKeyProvider                          keyProvider;

  public JwtTokenHandler() {
    mapper = new ObjectMapper().registerModule(new AfterburnerModule()).configure(
        DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    mapper.addMixInAnnotations(IJwtHeader.class, IJwtHeaderMixin.class);
    mapper.addMixInAnnotations(IJwtPayload.class, IJwtPayloadMixin.class);

    // .registerModule();
  }

  public void registerPayloadClass(String name, Class clazz) {
    payloadClasses.put(name, clazz);
  }

  @Override
  public void config(final OServer iServer, final OServerParameterConfiguration[] iParams) {
    serverInstance = iServer;

    for (OServerParameterConfiguration param : iParams) {
      if (param.name.equalsIgnoreCase("oAuth2Key")) {
        byte secret[] = Base64.getUrlDecoder().decode(param.value);
        keyProvider = new DefaultJwtKeyProvider(secret);
      }
    }

    this.registerPayloadClass("OrientDb", JwtTokenHandler.class);
  }

  @Override
  public IToken parseToken(byte[] tokenBytes) throws InvalidKeyException, NoSuchAlgorithmException, IOException {
    IToken token = null;

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

    byte[] decodedHeader = b64decoder.decode(tokenBytes, 0, firstDot);
    JwtHeader header = mapper.readValue(decodedHeader, JwtHeader.class);

    Mac mac = threadLocalMac.get();

    try {
      mac.init(getKeyProvider().getKey(header));
      mac.update(tokenBytes, 0, secondDot);
      byte[] calculatedSignature = mac.doFinal();

      byte[] decodedSignature = b64decoder.decode(tokenBytes, secondDot + 1, tokenBytes.length);

      boolean signatureValid = Arrays.equals(calculatedSignature, decodedSignature);

      if (signatureValid) {
        byte[] decodedPayload = b64decoder.decode(tokenBytes, firstDot + 1, secondDot);
        Class payloadClass = payloadClasses.get(header.getType());
        if (payloadClass == null) {
          throw new Exception("Payload class not registered:" + header.getType());
        }
        OrientJwtPayload payload = mapper.readValue(decodedPayload, OrientJwtPayload.class);
        token = new JsonWebToken(header, payload);
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
  public boolean validateToken(IToken token, String command, String database) {
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

  public byte[] getSignedToken(ODatabaseDocumentInternal db, OSecurityUser user) {
    ByteArrayOutputStream tokenByteOS = new ByteArrayOutputStream(1024);
    JwtHeader header = new JwtHeader();
    header.setAlgorithm("HS256");
    header.setKeyId("");
    header.setType("OrientDb");

    OrientJwtPayload payload = new OrientJwtPayload();
    payload.setAudience("OrientDb");
    payload.setDbName(db.getName());
    payload.setUserRid(user.getDocument().getIdentity().toString());

    payload.setAudience("Orient");
    long expiryMinutes = 60000 * 10;
    long currTime = System.currentTimeMillis();
    Date issueTime = new Date(currTime);
    Date expDate = new Date(currTime + expiryMinutes);
    payload.setIssuedAt(issueTime.getTime());
    payload.setNotBefore(issueTime.getTime());
    payload.setSubject(user.getName());
    payload.setTokenId(UUID.randomUUID().toString());
    payload.setExpiry(expDate.getTime());

    Mac mac = threadLocalMac.get();

    try {
      tokenByteOS.write(b64encoder.encode(mapper.writeValueAsBytes(header)));
      tokenByteOS.write(JWT_DELIMITER);
      tokenByteOS.write(b64encoder.encode(mapper.writeValueAsBytes(payload)));

      byte[] unsignedToken = tokenByteOS.toByteArray();

      tokenByteOS.write(JWT_DELIMITER);
      mac.init(getKeyProvider().getKey(header));
      tokenByteOS.write(b64encoder.encode(mac.doFinal(unsignedToken)));

    } catch (Exception ex) {
      OLogManager.instance().error(this, "Error signing token", ex);

    }

    return tokenByteOS.toByteArray();
  }

  @Override
  public String getName() {
    return "JwtTokenHandler";
  }

  protected IJwtKeyProvider getKeyProvider() {
    return keyProvider;
  }
}
