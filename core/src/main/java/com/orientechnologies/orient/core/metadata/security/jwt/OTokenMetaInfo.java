package com.orientechnologies.orient.core.metadata.security.jwt;

public interface OTokenMetaInfo {

  String getDbType(int pos);

  int getDbTypeID(String databaseType);
}
