package com.orientechnologies.orient.core.metadata.security;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.io.Serializable;
import java.util.Set;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientechnologies.com)
 * @since 03/11/14
 */
public interface OSecurityRole extends Serializable {
  public enum ALLOW_MODES {
    DENY_ALL_BUT, ALLOW_ALL_BUT
  }

  public boolean allow(final ORule.ResourceGeneric resourceGeneric, String resourceSpecific, final int iCRUDOperation);

  public boolean hasRule(final ORule.ResourceGeneric resourceGeneric, String resourceSpecific);

  public OSecurityRole addRule(final ORule.ResourceGeneric resourceGeneric, String resourceSpecific, final int iOperation);

  public OSecurityRole grant(final ORule.ResourceGeneric resourceGeneric, String resourceSpecific, final int iOperation);

  public OSecurityRole revoke(final ORule.ResourceGeneric resourceGeneric, String resourceSpecific, final int iOperation);

  @Deprecated
  public boolean allow(final String iResource, final int iCRUDOperation);

  @Deprecated
  public boolean hasRule(final String iResource);

  @Deprecated
  public OSecurityRole addRule(final String iResource, final int iOperation);

  @Deprecated
  public OSecurityRole grant(final String iResource, final int iOperation);

  @Deprecated
  public OSecurityRole revoke(final String iResource, final int iOperation);

  public String getName();

  public OSecurityRole.ALLOW_MODES getMode();

  public OSecurityRole setMode(final OSecurityRole.ALLOW_MODES iMode);

  public OSecurityRole getParentRole();

  public OSecurityRole setParentRole(final OSecurityRole iParent);

  public Set<ORule> getRuleSet();

  public OIdentifiable getIdentity();

  public ODocument getDocument();
}
