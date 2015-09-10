package com.orientechnologies.orient.server;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.metadata.security.OSecurityUser;
import com.orientechnologies.orient.core.metadata.security.OToken;
import com.orientechnologies.orient.server.network.protocol.ONetworkProtocolData;
import com.orientechnologies.orient.server.plugin.OServerPlugin;

/**
 * Created by emrul on 27/10/2014.
 *
 * @author Emrul Islam <emrul@emrul.com> Copyright 2014 Emrul Islam
 */
public interface OTokenHandler  {
  @Deprecated
  public static final String TOKEN_HANDLER_NAME = "OTokenHandler";

  // Return null if token is unparseable or fails verification.
  // The returned token should be checked to ensure isVerified == true.
  public OToken parseWebToken(byte tokenBytes[]) throws InvalidKeyException, NoSuchAlgorithmException, IOException;

  public OToken parseBinaryToken(byte tokenBytes[]) throws InvalidKeyException, NoSuchAlgorithmException, IOException;

  public boolean validateToken(OToken token, String command, String database);

  public boolean validateBinaryToken(OToken token);

  public ONetworkProtocolData getProtocolDataFromToken(OToken token);

  // Return a byte array representing a signed token
  public byte[] getSignedWebToken(ODatabaseDocumentInternal db, OSecurityUser user);

  public byte[] getSignedBinaryToken(ODatabaseDocumentInternal db, OSecurityUser user, ONetworkProtocolData data);

  public byte[] renewIfNeeded(OToken token);

  public boolean isEnabled();

}
