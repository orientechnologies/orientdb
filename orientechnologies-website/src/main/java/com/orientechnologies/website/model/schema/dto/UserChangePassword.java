package com.orientechnologies.website.model.schema.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Created by Enrico Risa on 04/07/2018.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class UserChangePassword {

  public UserChangePassword() {
  }

  String token;
  String password;
  String oldPassword;

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public String getToken() {
    return token;
  }

  public void setToken(String token) {
    this.token = token;
  }

  public String getOldPassword() {
    return oldPassword;
  }

  public void setOldPassword(String oldPassword) {
    this.oldPassword = oldPassword;
  }
}
