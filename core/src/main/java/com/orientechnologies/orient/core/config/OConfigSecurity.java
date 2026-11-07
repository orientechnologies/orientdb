package com.orientechnologies.orient.core.config;

import java.util.List;

public class OConfigSecurity {

  // False if what to get user from somewhere else, like LDAP.
  private boolean storeUsers;
  private boolean enabled;
  private boolean debug;
  private boolean storePasswords;
  private List<OConfigUser> users;
  private OConfigAuthentication authentication;
  private OConfigPasswordValidator passwordValidator;
  private OConfigSecurityAuditing auditing;
  private OLdapImportConfig ldapImporter;

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  public void setDebug(boolean debug) {
    this.debug = debug;
  }

  public void setAuditing(OConfigSecurityAuditing auditing) {
    this.auditing = auditing;
  }

  public void setStorePasswords(boolean storePasswords) {
    this.storePasswords = storePasswords;
  }

  public void setAuthentication(OConfigAuthentication authentication) {
    this.authentication = authentication;
  }

  public void setUsers(List<OConfigUser> users) {
    this.users = users;
  }

  public List<OConfigUser> getUsers() {
    return users;
  }

  public void setPasswordValidator(OConfigPasswordValidator passwordValidator) {
    this.passwordValidator = passwordValidator;
  }

  public void setLdapImporter(OLdapImportConfig ldapImporter) {
    this.ldapImporter = ldapImporter;
  }
}
