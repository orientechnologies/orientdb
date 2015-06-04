package com.orientechnologies.website.security;

import com.orientechnologies.website.model.schema.dto.OUser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.security.access.PermissionEvaluator;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Component;

import java.io.Serializable;

/**
 * Created by Enrico Risa on 04/06/15.
 */

@Component
public class PrjHubPermissionEvaluator implements PermissionEvaluator {

  @Autowired
  @Lazy
  private OSecurityManager securityManager;

  public PrjHubPermissionEvaluator() {
  }

  @Override
  public boolean hasPermission(Authentication authentication, Object o, Object o1) {
    return false;
  }

  @Override
  public boolean hasPermission(Authentication authentication, Serializable serializable, String s, Object o) {
    if (authentication instanceof DeveloperAuthentication) {
      DeveloperAuthentication auth = (DeveloperAuthentication) authentication;
      OUser user = auth.getUser();
      return securityManager.isMemberOrSupport(user, s);
    } else {
      return false;
    }
  }
}
