package com.orientechnologies.website.model.schema.dto;

/**
 * Created by Enrico Risa on 20/10/14.
 */
public class Developer {

  private String id;
  private String login;
  private String token;
  private String email;

  public Developer(String login, String token, String email) {
    this.login = login;
    this.token = token;
    this.email = email;
  }

  public Developer() {
  }

  public String getLogin() {
    return login;
  }

  public void setLogin(String login) {
    this.login = login;
  }

  public String getToken() {
    return token;
  }

  public void setToken(String token) {
    this.token = token;
  }

  public String getEmail() {
    return email;
  }

  public void setEmail(String email) {
    this.email = email;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }
}
