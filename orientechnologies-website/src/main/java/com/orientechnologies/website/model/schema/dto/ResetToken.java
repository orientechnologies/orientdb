package com.orientechnologies.website.model.schema.dto;

import com.orientechnologies.website.model.schema.Identity;

import java.util.Date;

/**
 * Created by Enrico Risa on 05/07/2018.
 */
public class ResetToken extends Identity {

  String token;
  Date   expire;
  OUser user;

  public String getToken() {
    return token;
  }

  public void setToken(String token) {
    this.token = token;
  }

  public Date getExpire() {
    return expire;
  }

  public void setExpire(Date expire) {
    this.expire = expire;
  }

  public OUser getUser() {
    return user;
  }

  public void setUser(OUser user) {
    this.user = user;
  }
}
