package com.orientechnologies.orient.server.jwt.mixin;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Created by emrul on 28/09/2014.
 *
 * @author Emrul Islam <emrul@emrul.com> Copyright 2014 Emrul Islam
 */
public interface OJwtHeaderMixin {

  @JsonProperty(value = "alg")
  public String getAlggorithm();

  @JsonProperty(value = "alg")
  public void setAlgorithm(String alg);

  @JsonProperty(value = "typ")
  public String getType();

  @JsonProperty(value = "typ")
  public void setType(String typ);

  @JsonProperty(value = "kid")
  public String getKeyId();

  @JsonProperty(value = "kid")
  public void setKeyId(String kid);

}
