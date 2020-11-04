package com.orientechnologies.orient.core.metadata.security.jwt;

public interface OBinaryTokenPayload extends OTokenPayload {
  short getProtocolVersion();

  String getSerializer();

  String getDriverName();

  String getDriverVersion();

  boolean isServerUser();
}
