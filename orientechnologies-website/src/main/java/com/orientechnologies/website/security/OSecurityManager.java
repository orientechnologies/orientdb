package com.orientechnologies.website.security;

import com.orientechnologies.website.helper.SecurityHelper;
import com.orientechnologies.website.model.schema.dto.Client;
import com.orientechnologies.website.repository.OrganizationRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

import com.orientechnologies.website.model.schema.dto.OUser;
import com.orientechnologies.website.services.UserService;

import java.util.List;

/**
 * Created by Enrico Risa on 01/06/15.
 */
@Component
public class OSecurityManager {

  @Autowired
  protected UserService            userService;

  @Autowired
  protected OrganizationRepository organizationRepository;

  public boolean isMember(OUser user, String organization) {
    return userService.isMember(user, organization);
  }

  public boolean isSupport(OUser user, String organization) {
    return userService.isSupport(user, organization);
  }

  public boolean isMemberOrSupport(OUser user, String organization) {

    return userService.isMember(user, organization) || userService.isSupport(user, organization);
  }

  public boolean isCurrentMember(String organization) {
    return isMember(SecurityHelper.currentUser(), organization);
  }

  public boolean isCurrentSupport(String organization) {
    return isSupport(SecurityHelper.currentUser(), organization);
  }

  public boolean isCurrentMemberOrSupport(String organization) {
    return isMemberOrSupport(SecurityHelper.currentUser(), organization);
  }

  public boolean isCurrentClient(String organization, OUser user, Client c) {

    Client client = userService.getClient(user, organization);

    if (c != null && client != null) {
      return c.getClientId() == client.getClientId();
    }
    return false;
  }

  public OUser bot(String organization) {
    List<OUser> bots = organizationRepository.findBots(organization);
    if (bots.size() > 0) {
      return bots.iterator().next();
    }
    return null;
  }

  public OUser botIfSupport(String organization) {
    OUser user = currentUser();
    if (isSupport(user, organization)) {
      OUser tmp = bot(organization);
      if (tmp != null) {
        user = tmp;
      }
    }
    return user;
  }

  public OUser currentUser() {
    Authentication auth = SecurityContextHolder.getContext().getAuthentication();
    DeveloperAuthentication developerAuthentication = (DeveloperAuthentication) auth;
    return developerAuthentication.getUser();
  }

  public String currentToken() {
    Authentication auth = SecurityContextHolder.getContext().getAuthentication();
    DeveloperAuthentication developerAuthentication = (DeveloperAuthentication) auth;
    return developerAuthentication.getGithubToken();
  }

}
