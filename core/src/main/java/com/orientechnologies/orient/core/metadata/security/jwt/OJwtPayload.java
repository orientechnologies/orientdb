package com.orientechnologies.orient.core.metadata.security.jwt;

/**
 * Created by emrul on 28/09/2014.
 *
 * @author Emrul Islam <emrul@emrul.com> Copyright 2014 Emrul Islam
 */
public interface OJwtPayload {

  public String getIssuer();

  public void setIssuer(String iss);

  public long getExpiry();

  public void setExpiry(long exp);

  public long getIssuedAt();

  public void setIssuedAt(long iat);

  public long getNotBefore();

  public void setNotBefore(long nbf);

  public String getUserName();

  public void setUserName(String sub);

  public String getAudience();

  public void setAudience(String aud);

  public String getTokenId();

  public void setTokenId(String jti);

  public void setDatabase(String database);

  public String getDatabase();

  public void setDatabaseType(String databaseType);

  public String getDatabaseType();
}
