package com.orientechnologies.orient.core.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

@JsonInclude(Include.NON_NULL)
public class OConfigNetworkTls {
  private boolean clientAuth = false;
  private String keyStorePath;
  private String keyStorePassword;
  private String keyStoreType;
  private String trustStorePath;
  private String trustStorePassword;
  private String trustStoreType;

  public boolean isClientAuth() {
    return clientAuth;
  }

  public String getKeyStorePath() {
    return keyStorePath;
  }

  public String getKeyStorePassword() {
    return keyStorePassword;
  }

  public String getKeyStoreType() {
    return keyStoreType;
  }

  public String getTrustStorePath() {
    return trustStorePath;
  }

  public String getTrustStorePassword() {
    return trustStorePassword;
  }

  public String getTrustStoreType() {
    return trustStoreType;
  }

  public void setClientAuth(boolean clientAuth) {
    this.clientAuth = clientAuth;
  }

  public void setKeyStorePath(String keyStorePath) {
    this.keyStorePath = keyStorePath;
  }

  public void setKeyStorePassword(String keyStorePassword) {
    this.keyStorePassword = keyStorePassword;
  }

  public void setKeyStoreType(String keyStoreType) {
    this.keyStoreType = keyStoreType;
  }

  public void setTrustStorePath(String trustStorePath) {
    this.trustStorePath = trustStorePath;
  }

  public void setTrustStorePassword(String trustStorePassword) {
    this.trustStorePassword = trustStorePassword;
  }

  public void setTrustStoreType(String trustStoreType) {
    this.trustStoreType = trustStoreType;
  }
}
