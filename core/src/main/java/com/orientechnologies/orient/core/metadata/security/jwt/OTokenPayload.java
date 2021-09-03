package com.orientechnologies.orient.core.metadata.security.jwt;

import com.orientechnologies.orient.core.id.ORID;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

public interface OTokenPayload {

  String getDatabase();

  long getExpiry();

  ORID getUserRid();

  String getDatabaseType();

  String getUserName();

  void setExpiry(long expiry);

  String getPayloadType();

  void serialize(DataOutputStream output, OTokenMetaInfo serializer)
      throws UnsupportedEncodingException, IOException;
}
