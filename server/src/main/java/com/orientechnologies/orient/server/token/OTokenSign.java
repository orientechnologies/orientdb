package com.orientechnologies.orient.server.token;

import com.orientechnologies.orient.core.metadata.security.jwt.OTokenHeader;
import com.orientechnologies.orient.server.OParsedToken;

public interface OTokenSign {

  byte[] signToken(OTokenHeader header, byte[] unsignedToken);

  boolean verifyTokenSign(OParsedToken parsed);

  String getAlgorithm();

  String getDefaultKey();

  String[] getKeys();
}
