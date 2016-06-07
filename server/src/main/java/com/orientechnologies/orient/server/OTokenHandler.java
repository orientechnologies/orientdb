package com.orientechnologies.orient.server;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.metadata.security.OSecurityUser;
import com.orientechnologies.orient.core.metadata.security.OToken;
import com.orientechnologies.orient.server.network.protocol.ONetworkProtocolData;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

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
  OToken parseWebToken(byte tokenBytes[]) throws InvalidKeyException, NoSuchAlgorithmException, IOException;

  OToken parseBinaryToken(byte tokenBytes[]) throws InvalidKeyException, NoSuchAlgorithmException, IOException;

  boolean validateToken(OToken token, String command, String database);

  boolean validateBinaryToken(OToken token);

  ONetworkProtocolData getProtocolDataFromToken(OClientConnection oClientConnection, OToken token);

  // Return a byte array representing a signed token
  byte[] getSignedWebToken(ODatabaseDocument db, OSecurityUser user);

  byte[] getSignedBinaryToken(ODatabaseDocumentInternal db, OSecurityUser user, ONetworkProtocolData data);

  byte[] renewIfNeeded(OToken token);

  boolean isEnabled();

}
