package com.orientechnologies.orient.server.jwt.mixin;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Created by emrul on 28/09/2014.
 *
 * @author Emrul Islam <emrul@emrul.com> Copyright 2014 Emrul Islam
 */
public interface OJwtPayloadMixin {

  @JsonProperty(value = "iss")
  public String getIssuer();

  @JsonProperty(value = "iss")
  public void setIssuer(String iss);

  @JsonProperty(value = "exp")
  public long getExpiry();

  @JsonProperty(value = "exp")
  public void setExpiry(long exp);

  @JsonProperty(value = "iat")
  public long getIssuedAt();

  @JsonProperty(value = "iat")
  public void setIssuedAt(long iat);

  @JsonProperty(value = "nbf")
  public long getNotBefore();

  @JsonProperty(value = "nbf")
  public void setNotBefore(long nbf);

  @JsonProperty(value = "sub")
  public String getSubject();

  @JsonProperty(value = "sub")
  public void setSubject(String sub);

  @JsonProperty(value = "aud")
  public String getAudience();

  @JsonProperty(value = "aud")
  public void setAudience(String aud);

  @JsonProperty(value = "jti")
  public String getTokenId();

  @JsonProperty(value = "jti")
  public void setTokenId(String jti);
}
