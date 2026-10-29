package com.orientechnologies.security.ldap;

import com.orientechnologies.orient.core.security.OSecurityComponent;
import com.orientechnologies.orient.core.security.OSecurityComponentLoader;

public class OLDAPImporterLoader implements OSecurityComponentLoader {

  @Override
  public String getName() {
    return "ldap-importer";
  }

  @Override
  public String getClassName() {
    return OLDAPImporter.class.getName();
  }

  @Override
  public OSecurityComponent newInstance() {
    return new OLDAPImporter();
  }
}
