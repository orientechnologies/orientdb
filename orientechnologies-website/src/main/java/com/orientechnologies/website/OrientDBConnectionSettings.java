package com.orientechnologies.website;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Created by Enrico Risa on 17/10/14.
 */
@Component
@ConfigurationProperties(prefix = "connection")
public class OrientDBConnectionSettings {

  private String url;
  private String usr;
  private String pwd;

  public String getUrl() {
    return url;
  }

  public void setUrl(String url) {
    this.url = url;
  }

  public String getUsr() {
    return usr;
  }

  public void setUsr(String usr) {
    this.usr = usr;
  }

  public String getPwd() {
    return pwd;
  }

  public void setPwd(String pwd) {
    this.pwd = pwd;
  }
}
