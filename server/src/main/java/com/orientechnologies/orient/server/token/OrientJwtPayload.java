package com.orientechnologies.orient.server.token;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.security.jwt.OJwtPayload;
import com.orientechnologies.orient.core.metadata.security.jwt.OTokenMetaInfo;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

/**
 * Created by emrul on 28/09/2014.
 *
 * @author Emrul Islam <emrul@emrul.com> Copyright 2014 Emrul Islam
 */
public class OrientJwtPayload implements OJwtPayload {
  public String iss;
  public String sub;
  public String aud;
  public String jti;
  public long exp;
  public long iat;
  public long nbf;
  public ORID userRid;
  public String database;
  public String databaseType;

  @Override
  public String getIssuer() {
    return iss;
  }

  @Override
  public void setIssuer(String iss) {
    this.iss = iss;
  }

  @Override
  public long getExpiry() {
    return exp;
  }

  @Override
  public void setExpiry(long exp) {
    this.exp = exp;
  }

  @Override
  public long getIssuedAt() {
    return iat;
  }

  @Override
  public void setIssuedAt(long iat) {
    this.iat = iat;
  }

  @Override
  public long getNotBefore() {
    return nbf;
  }

  @Override
  public void setNotBefore(long nbf) {
    this.nbf = nbf;
  }

  @Override
  public String getUserName() {
    return sub;
  }

  @Override
  public void setUserName(String sub) {
    this.sub = sub;
  }

  @Override
  public String getAudience() {
    return aud;
  }

  @Override
  public void setAudience(String aud) {
    this.aud = aud;
  }

  @Override
  public String getTokenId() {
    return jti;
  }

  @Override
  public void setTokenId(String jti) {
    this.jti = jti;
  }

  public ORID getUserRid() {
    return userRid;
  }

  public void setUserRid(ORID userRid) {
    this.userRid = userRid;
  }

  public String getDatabase() {
    return database;
  }

  public void setDatabase(String dbName) {
    this.database = dbName;
  }

  @Override
  public String getDatabaseType() {
    return databaseType;
  }

  @Override
  public void setDatabaseType(String databaseType) {
    this.databaseType = databaseType;
  }

  @Override
  public String getPayloadType() {
    return "";
  }

  @Override
  public void serialize(DataOutputStream output, OTokenMetaInfo serializer)
      throws UnsupportedEncodingException, IOException {
    throw new UnsupportedOperationException();
  }
}
