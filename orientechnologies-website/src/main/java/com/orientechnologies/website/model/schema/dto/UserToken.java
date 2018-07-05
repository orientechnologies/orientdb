package com.orientechnologies.website.model.schema.dto;

/**
 * Created by Enrico Risa on 04/07/2018.
 */
public class UserToken {

  private String token;

  public UserToken() {
  }

  public UserToken(String login) {
    this.token = login;
  }

  public String getToken() {
    return token;
  }

  public void setToken(String token) {
    this.token = token;
  }
}
