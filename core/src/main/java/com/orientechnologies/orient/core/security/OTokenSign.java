package com.orientechnologies.orient.core.security;

import com.orientechnologies.orient.core.metadata.security.jwt.OTokenHeader;

public interface OTokenSign {

  byte[] signToken(OTokenHeader header, byte[] unsignedToken);

  boolean verifyTokenSign(OParsedToken parsed);

  String getAlgorithm();

  String getDefaultKey();

  String[] getKeys();
}
