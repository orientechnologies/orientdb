package com.orientechnologies.orient.core.security;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.exception.OSystemException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.metadata.security.OToken;
import com.orientechnologies.orient.core.metadata.security.jwt.OKeyProvider;
import com.orientechnologies.orient.core.metadata.security.jwt.OTokenHeader;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import javax.crypto.Mac;

public class OTokenSignImpl implements OTokenSign {

  public static final String ENCRYPTION_ALGORITHM_DEFAULT = "HmacSHA256";

  private String algorithm = ENCRYPTION_ALGORITHM_DEFAULT;

  private static final ThreadLocal<Map<String, Mac>> threadLocalMac = new MacThreadLocal();

  private final OKeyProvider keyProvider;

  private static class MacThreadLocal extends ThreadLocal<Map<String, Mac>> {
    @Override
    protected Map<String, Mac> initialValue() {
      return new HashMap<String, Mac>();
    }
  }

  public OTokenSignImpl(OContextConfiguration config) {
    this(
        OTokenSignImpl.readKeyFromConfig(config),
        config.getValueAsString(OGlobalConfiguration.NETWORK_TOKEN_ENCRYPTION_ALGORITHM));
  }

  public OTokenSignImpl(byte[] key, String algorithm) {
    this.keyProvider = new DefaultKeyProvider(key);
    if (algorithm != null) {
      this.algorithm = algorithm;
    }
    try {
      Mac.getInstance(this.algorithm);
    } catch (NoSuchAlgorithmException nsa) {
      throw new IllegalArgumentException(
          "Can't find encryption algorithm '" + algorithm + "'", nsa);
    }
  }

  private Mac getLocalMac() {
    Map<String, Mac> map = threadLocalMac.get();
    Mac mac = map.get(this.algorithm);
    if (mac == null) {
      try {
        mac = Mac.getInstance(this.algorithm);
      } catch (NoSuchAlgorithmException nsa) {
        throw new IllegalArgumentException(
            "Can't find encryption algorithm '" + algorithm + "'", nsa);
      }
      map.put(this.algorithm, mac);
    }
    return mac;
  }

  @Override
  public byte[] signToken(final OTokenHeader header, final byte[] unsignedToken) {
    final Mac mac = getLocalMac();
    try {
      mac.init(keyProvider.getKey(header));
      return mac.doFinal(unsignedToken);
    } catch (Exception ex) {
      throw OException.wrapException(new OSystemException("Error on token parsing"), ex);
    } finally {
      mac.reset();
    }
  }

  @Override
  public boolean verifyTokenSign(OParsedToken parsed) {
    OToken token = parsed.getToken();
    byte[] tokenBytes = parsed.getTokenBytes();
    byte[] signature = parsed.getSignature();
    final Mac mac = getLocalMac();

    try {
      mac.init(keyProvider.getKey(token.getHeader()));
      mac.update(tokenBytes, 0, tokenBytes.length);
      final byte[] calculatedSignature = mac.doFinal();
      boolean valid = MessageDigest.isEqual(calculatedSignature, signature);
      if (!valid) {
        OLogManager.instance()
            .warn(
                this,
                "Token signature failure: %s",
                Base64.getEncoder().encodeToString(tokenBytes));
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

  @Override
  public String getAlgorithm() {
    return algorithm;
  }

  @Override
  public String getDefaultKey() {
    return this.keyProvider.getDefaultKey();
  }

  @Override
  public String[] getKeys() {
    return this.keyProvider.getKeys();
  }

  public static byte[] readKeyFromConfig(OContextConfiguration config) {
    byte[] key = null;
    String configKey = config.getValueAsString(OGlobalConfiguration.NETWORK_TOKEN_SECRETKEY);
    if (configKey == null || configKey.length() == 0)
      configKey = config.getValueAsString(OGlobalConfiguration.OAUTH2_SECRETKEY);

    if (configKey != null && configKey.length() > 0) key = Base64.getUrlDecoder().decode(configKey);

    if (key == null) {
      key = OSecurityManager.digestSHA256(String.valueOf(new SecureRandom().nextLong()));
    }
    return key;
  }
}
