package com.orientechnologies.website.model.schema.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.userdetails.UserDetails;

import java.util.Collection;

/**
 * Created by Enrico Risa on 20/10/14.
 */

@JsonIgnoreProperties(ignoreUnknown = true)
public class OUser implements UserDetails {

  @JsonIgnore
  private String  rid;
  private Long    id;
  private String  name;
  @JsonIgnore
  private String  token;
  @JsonIgnore
  private String  email;

  private String  firstName;

  private String  secondName;
  private String  company;

  private Boolean notification;
  private Boolean watching;
  private Boolean chatNotification;

  private Boolean isClient;

  private String  clientName;

  private Integer clientId;


  public Boolean getNotification() {
    return notification;
  }

  public void setNotification(Boolean notification) {
    this.notification = notification;
  }

  public String getWorkingEmail() {
    return workingEmail;
  }

  public void setWorkingEmail(String workingEmail) {
    this.workingEmail = workingEmail;
  }

  public String getFirstName() {
    return firstName;
  }

  public void setFirstName(String firstName) {
    this.firstName = firstName;
  }

  public String getSecondName() {
    return secondName;
  }

  public void setSecondName(String secondName) {
    this.secondName = secondName;
  }

  public String getCompany() {
    return company;
  }

  public void setCompany(String company) {
    this.company = company;
  }

  private String  workingEmail;
  private Boolean confirmed;

  public OUser(String name, String token, String email) {
    this.name = name;
    this.token = token;
    this.email = email;
  }

  public OUser() {
  }

  public void setId(Long id) {
    this.id = id;
  }

  public Long getId() {
    return id;
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

  public String getRid() {
    return rid;
  }

  public void setRid(String rid) {
    this.rid = rid;
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

  public Boolean getConfirmed() {
    return confirmed;
  }

  public void setConfirmed(Boolean confirmed) {
    this.confirmed = confirmed;
  }

  public Boolean getWatching() {
    return watching;
  }

  public void setWatching(Boolean watching) {
    this.watching = watching;
  }

  public Boolean getChatNotification() {
    return chatNotification;

  }

  public Boolean getIsClient() {
    return isClient;
  }

  public void setIsClient(Boolean isClient) {
    this.isClient = isClient;
  }

  public void setClientName(String clientName) {
    this.clientName = clientName;
  }

  public String getClientName() {
    return clientName;
  }

  public Integer getClientId() {
    return clientId;
  }

  public void setClientId(Integer clientId) {
    this.clientId = clientId;
  }

  public void setChatNotification(Boolean chatNotification) {
    this.chatNotification = chatNotification;
  }


  public String getLogin() {
    return getName();
  }

  @Override
  public String toString() {
    return getName();
  }
}
