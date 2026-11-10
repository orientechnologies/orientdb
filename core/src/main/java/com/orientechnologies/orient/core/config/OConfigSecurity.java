package com.orientechnologies.orient.core.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import java.util.List;

@JsonInclude(Include.NON_NULL)
public class OConfigSecurity {

  // False if what to get user from somewhere else, like LDAP.
  private Boolean storeUsers;
  private Boolean enabled;
  private Boolean debug;
  private Boolean storePasswords;
  private List<OConfigUser> users;
  private OConfigAuthentication authentication;
  private OConfigPasswordValidator passwordValidator;
  private OConfigSecurityAuditing auditing;
  private OLdapImportConfig ldapImporter;

  public OConfigSecurity() {}

  public Boolean getStoreUsers() {
    return storeUsers;
  }

  public void setStoreUsers(Boolean storeUsers) {
    this.storeUsers = storeUsers;
  }

  public Boolean getEnabled() {
    return enabled;
  }

  public void setEnabled(Boolean enabled) {
    this.enabled = enabled;
  }

  public Boolean getDebug() {
    return debug;
  }

  public void setDebug(Boolean debug) {
    this.debug = debug;
  }

  public Boolean getStorePasswords() {
    return storePasswords;
  }

  public void setStorePasswords(Boolean storePasswords) {
    this.storePasswords = storePasswords;
  }

  public List<OConfigUser> getUsers() {
    return users;
  }

  public void setUsers(List<OConfigUser> users) {
    this.users = users;
  }

  public OConfigAuthentication getAuthentication() {
    return authentication;
  }

  public void setAuthentication(OConfigAuthentication authentication) {
    this.authentication = authentication;
  }

  public OConfigPasswordValidator getPasswordValidator() {
    return passwordValidator;
  }

  public void setPasswordValidator(OConfigPasswordValidator passwordValidator) {
    this.passwordValidator = passwordValidator;
  }

  public OConfigSecurityAuditing getAuditing() {
    return auditing;
  }

  public void setAuditing(OConfigSecurityAuditing auditing) {
    this.auditing = auditing;
  }

  public OLdapImportConfig getLdapImporter() {
    return ldapImporter;
  }

  public void setLdapImporter(OLdapImportConfig ldapImporter) {
    this.ldapImporter = ldapImporter;
  }
}
