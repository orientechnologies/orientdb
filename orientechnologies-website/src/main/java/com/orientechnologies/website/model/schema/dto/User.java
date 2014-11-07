package com.orientechnologies.website.model.schema.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.userdetails.UserDetails;

import java.util.Collection;

/**
 * Created by Enrico Risa on 20/10/14.
 */
public class User implements UserDetails {

  @JsonIgnore
  private String id;
  private String name;
  @JsonIgnore
  private String token;
  @JsonIgnore
  private String email;

  public User(String name, String token, String email) {
    this.name = name;
    this.token = token;
    this.email = email;
  }

  public User() {
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
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

  @JsonIgnore
  @Override
  public Collection<? extends GrantedAuthority> getAuthorities() {
    return null;
  }

  @JsonIgnore
  @Override
  public String getPassword() {
    return null;
  }

  @JsonIgnore
  @Override
  public String getUsername() {
    return getName();
  }

  @JsonIgnore
  @Override
  public boolean isAccountNonExpired() {
    return false;
  }

  @JsonIgnore
  @Override
  public boolean isAccountNonLocked() {
    return false;
  }

  @JsonIgnore
  @Override
  public boolean isCredentialsNonExpired() {
    return false;
  }

  @JsonIgnore
  @Override
  public boolean isEnabled() {
    return false;
  }
}
