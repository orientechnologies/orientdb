package com.orientechnologies.orient.core.metadata.security;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.io.Serializable;
import java.util.Set;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientechnologies.com)
 * @since 03/11/14
 */
public interface OSecurityUser extends Serializable {
  enum STATUSES {
    SUSPENDED, ACTIVE
  }

  OSecurityRole allow(final ORule.ResourceGeneric resourceGeneric, String resourceSpecific, final int iOperation);

  OSecurityRole checkIfAllowed(final ORule.ResourceGeneric resourceGeneric, String resourceSpecific, final int iOperation);

  boolean isRuleDefined(final ORule.ResourceGeneric resourceGeneric, String resourceSpecific);

  @Deprecated
  OSecurityRole allow(final String iResource, final int iOperation);

  @Deprecated
  OSecurityRole checkIfAllowed(final String iResource, final int iOperation);

  @Deprecated
  boolean isRuleDefined(final String iResource);

  boolean checkPassword(final String iPassword);

  String getName();

  OSecurityUser setName(final String iName);

  String getPassword();

  OSecurityUser setPassword(final String iPassword);

  OSecurityUser.STATUSES getAccountStatus();

  void setAccountStatus(OSecurityUser.STATUSES accountStatus);

  Set<? extends OSecurityRole> getRoles();

  OSecurityUser addRole(final String iRole);

  OSecurityUser addRole(final OSecurityRole iRole);

  boolean removeRole(final String iRoleName);

  boolean hasRole(final String iRoleName, final boolean iIncludeInherited);

  OIdentifiable getIdentity();

  ODocument getDocument();
}
