package com.orientechnologies.website.security;

import com.orientechnologies.website.model.schema.dto.OUser;
import com.orientechnologies.website.model.schema.dto.UserAuthority;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;

import java.util.Collection;
import java.util.HashSet;

/**
 * Created by Enrico Risa on 21/10/14.
 */
public class DeveloperAuthentication implements Authentication {

  private final OUser user;
  private boolean         authenticated = true;

  public DeveloperAuthentication(OUser user) {
    this.user = user;
  }

  @Override
  public Collection<? extends GrantedAuthority> getAuthorities() {
    return new HashSet<GrantedAuthority>() {
      {
        add(UserAuthority.baseDevelAuthority());
      }
    };
  }

  @Override
  public Object getCredentials() {
    return null;
  }

  @Override
  public Object getDetails() {
    return null;
  }

  @Override
  public Object getPrincipal() {
    return null;
  }

  @Override
  public boolean isAuthenticated() {
    return authenticated;
  }

  @Override
  public void setAuthenticated(boolean b) throws IllegalArgumentException {
    authenticated = b;
  }

  public String getGithubToken() {
    return user.getToken();
  }

  @Override
  public String getName() {
    return user.getName();
  }

  public OUser getUser() {
    return user;
  }
}
