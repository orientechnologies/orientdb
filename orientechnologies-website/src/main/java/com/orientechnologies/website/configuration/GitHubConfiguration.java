package com.orientechnologies.website.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

/**
 * Created by Enrico Risa on 20/10/14.
 */
@Component
@ConfigurationProperties(prefix = "github")
@PropertySource("classpath:github-${spring.profiles.active}.properties")
public class GitHubConfiguration {

  int             port = 8080;

  private boolean mock = false;
  private String  clientId;

  private String  loginUrl;
  private String  clientSecret;

  private String  authorizeRedirect;

  private String  accessRedirect;

  private String  api;

  public String getAuthorizeRedirect() {
    return authorizeRedirect;
  }

  public void setAuthorizeRedirect(String authorizeRedirect) {
    this.authorizeRedirect = authorizeRedirect;
  }

  public String getAccessRedirect() {
    return accessRedirect;
  }

  public void setAccessRedirect(String accessRedirect) {
    this.accessRedirect = accessRedirect;
  }

  public String getClientId() {
    return clientId;
  }

  public void setClientId(String clientId) {
    this.clientId = clientId;
  }

  public String getLoginUrl() {
    return loginUrl;
  }

  public void setLoginUrl(String loginUrl) {
    this.loginUrl = loginUrl;
  }

  public String getClientSecret() {
    return clientSecret;
  }

  public void setClientSecret(String clientSecret) {
    this.clientSecret = clientSecret;
  }

  public void setMock(boolean mock) {
    this.mock = mock;
  }

  public boolean isMock() {
    return mock;
  }

  public String getApi() {
    if (mock) {
      return api.replace("${port}", "" + port);
    } else {
      return api;
    }

  }

  public void setPort(int port) {
    this.port = port;
  }

  public void setApi(String api) {
    this.api = api;

  }
}
