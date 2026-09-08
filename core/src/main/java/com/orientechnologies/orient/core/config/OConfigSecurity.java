package com.orientechnologies.orient.core.config;

import java.util.List;

public class OConfigSecurity {

  // False if what to get user from somewhere else, like LDAP.
  private boolean storeUsers;
  private List<OConfigUser> users;
  private List<String> authenticators;
  private OConfigPasswordValidator passwordValidator;
  private OConfigSecurityAuditing auditing;
}
