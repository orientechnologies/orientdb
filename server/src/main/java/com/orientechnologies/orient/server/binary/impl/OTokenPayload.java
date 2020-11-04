package com.orientechnologies.orient.server.binary.impl;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.server.token.OBinaryTokenSerializer;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

public interface OTokenPayload {

  String getDatabase();

  long getExpiry();

  ORID getUserRid();

  String getDatabaseType();

  short getProtocolVersion();

  String getSerializer();

  String getDriverName();

  String getDriverVersion();

  boolean isServerUser();

  String getUserName();

  void setExpiry(long expiry);

  String getPayloadType();

  void serialize(DataOutputStream output, OBinaryTokenSerializer serializer)
      throws UnsupportedEncodingException, IOException;
}
