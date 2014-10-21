package com.orientechnologies.website.model.schema.dto;

import org.springframework.security.core.GrantedAuthority;

/**
 * Created by Enrico Risa on 21/10/14.
 */
public class DeveloperAuthority implements GrantedAuthority {

  private String authority;

  public DeveloperAuthority(String authority) {
    this.authority = authority;
  }

  @Override
  public String getAuthority() {
    return authority;
  }

  public static DeveloperAuthority baseDevelAuthority() {
    return new DeveloperAuthority("DEVELOPER");
  }
}
