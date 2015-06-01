package com.orientechnologies.website.security;

import com.orientechnologies.website.helper.SecurityHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.orientechnologies.website.model.schema.dto.OUser;
import com.orientechnologies.website.services.UserService;

/**
 * Created by Enrico Risa on 01/06/15.
 */
@Component
public class SecurityManager {

  @Autowired
  protected UserService userService;

  public boolean isCurrentMember(String organization) {
    return userService.isMember(SecurityHelper.currentUser(), organization);
  }

  public boolean isCurrentMemberOrSupport(String organization) {
    OUser oUser = SecurityHelper.currentUser();
    return userService.isMember(oUser, organization) || userService.isSupport(oUser, organization);
  }
}
