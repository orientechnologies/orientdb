package com.orientechnologies.orient.server.token;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.metadata.security.OSecurityUser;
import com.orientechnologies.orient.core.metadata.security.OToken;
import com.orientechnologies.orient.server.OClientConnection;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OTokenHandler;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.network.protocol.ONetworkProtocolData;
import com.orientechnologies.orient.server.plugin.OServerPlugin;
import com.orientechnologies.orient.server.plugin.OServerPluginAbstract;

import javax.crypto.Mac;
import java.security.NoSuchAlgorithmException;

/**
 * Created by emrul on 27/10/2014.
 *
 * @author Emrul Islam <emrul@emrul.com> Copyright 2014 Emrul Islam
 */
public class OrientTokenHandler extends OServerPluginAbstract implements OTokenHandler, OServerPlugin {
  public static final String SIGN_KEY_PAR             = "oAuth2Key";
  public static final String SESSION_LENGHT_PAR       = "sessionLength";
  public static final String ENCRYPTION_ALGORITHM_PAR = "encryptionAlgorithm";

  public OrientTokenHandler() {
  }

  @Override
  public void config(final OServer iServer, final OServerParameterConfiguration[] iParams) {
    String key = null;
    Long baseSession = null;
    String algorithm = null;
    for (OServerParameterConfiguration param : iParams) {
      if (param.name.equalsIgnoreCase("enabled")) {
        if (!Boolean.parseBoolean(param.value))
          return;
      } else if (param.name.equalsIgnoreCase(SIGN_KEY_PAR)) {
        key = param.value;
      } else if (param.name.equalsIgnoreCase(SESSION_LENGHT_PAR)) {
        baseSession = Long.parseLong(param.value);
      } else if (param.name.equalsIgnoreCase(ENCRYPTION_ALGORITHM_PAR)) {
        algorithm = param.value;
        try {
          Mac.getInstance(algorithm);
        } catch (NoSuchAlgorithmException nsa) {
          throw new IllegalArgumentException("Cannot find encryption algorithm '" + algorithm + "'", nsa);
        }
      }
    }
    if (key != null)
      OGlobalConfiguration.NETWORK_TOKEN_SECRETKEY.setValue(key);
    if (baseSession != null)
      OGlobalConfiguration.NETWORK_TOKEN_EXPIRE_TIMEOUT.setValue(baseSession);
    if (algorithm != null)
      OGlobalConfiguration.NETWORK_TOKEN_ENCRIPTION_ALGORITHM.setValue(algorithm);

  }

  @Override
  public OToken parseWebToken(byte[] tokenBytes) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean validateToken(final OToken token, final String command, final String database) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean validateBinaryToken(final OToken token) {
    throw new UnsupportedOperationException();
  }

  public byte[] getSignedWebToken(final ODatabaseDocument db, final OSecurityUser user) {
    throw new UnsupportedOperationException();
  }

  public byte[] getSignedBinaryToken(final ODatabaseDocumentInternal db, final OSecurityUser user,
      final ONetworkProtocolData data) {
    throw new UnsupportedOperationException();
  }

  public ONetworkProtocolData getProtocolDataFromToken(OClientConnection oClientConnection, final OToken token) {
    throw new UnsupportedOperationException();
  }

  public OToken parseBinaryToken(final byte[] binaryToken) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getName() {
    return OTokenHandler.TOKEN_HANDLER_NAME;
  }

  @Override
  public byte[] renewIfNeeded(final OToken token) {
    throw new UnsupportedOperationException();
  }

  public long getSessionInMills() {
    throw new UnsupportedOperationException();
  }

  public boolean isEnabled() {
    return true;
  }
}
