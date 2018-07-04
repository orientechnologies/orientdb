package com.orientechnologies.website.model.schema.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Created by Enrico Risa on 04/07/2018.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class UserRegistration {

  public UserRegistration() {
  }

  String name;
  String password;
  String email;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public String getEmail() {
    return email;
  }

  public void setEmail(String email) {
    this.email = email;
  }
}
