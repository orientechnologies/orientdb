package com.orientechnologies.orient.core.metadata.security;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientechnologies.com)
 * @since 03/11/14
 */
public class OImmutableRole implements OSecurityRole {
  private static final long                       serialVersionUID = 1L;
  private final ALLOW_MODES                       mode;
  private final OSecurityRole                     parentRole;

  private final Map<ORule.ResourceGeneric, ORule> rules            = new HashMap<ORule.ResourceGeneric, ORule>();
  private final String                            name;
  private final ORID                              rid;
  private final ORole                             role;

  public OImmutableRole(ORole role) {
    if (role.getParentRole() == null)
      this.parentRole = null;
    else
      this.parentRole = new OImmutableRole(role.getParentRole());

    this.mode = role.getMode();
    this.name = role.getName();
    this.rid = role.getIdentity().getIdentity();
    this.role = role;

    for (ORule rule : role.getRuleSet())
      rules.put(rule.getResourceGeneric(), rule);

  }

  public boolean allow(final ORule.ResourceGeneric resourceGeneric, String resourceSpecific, final int iCRUDOperation) {
    final ORule rule = rules.get(resourceGeneric);
    if (rule != null) {
      final Boolean allowed = rule.isAllowed(resourceSpecific, iCRUDOperation);
      if (allowed != null)
        return allowed;
    }

    if (parentRole != null)
      // DELEGATE TO THE PARENT ROLE IF ANY
      return parentRole.allow(resourceGeneric, resourceSpecific, iCRUDOperation);

    return mode == ALLOW_MODES.ALLOW_ALL_BUT;
  }

  public boolean hasRule(final ORule.ResourceGeneric resourceGeneric, String resourceSpecific) {
    ORule rule = rules.get(resourceGeneric);

    if (rule == null)
      return false;

    if (resourceSpecific != null && !rule.containsSpecificResource(resourceSpecific))
      return false;

    return true;
  }

  public OSecurityRole addRule(final ORule.ResourceGeneric resourceGeneric, String resourceSpecific, final int iOperation) {
    throw new UnsupportedOperationException();
  }

  public OSecurityRole grant(final ORule.ResourceGeneric resourceGeneric, String resourceSpecific, final int iOperation) {
    throw new UnsupportedOperationException();
  }

  public ORole revoke(final ORule.ResourceGeneric resourceGeneric, String resourceSpecific, final int iOperation) {
    throw new UnsupportedOperationException();
  }

  @Deprecated
  @Override
  public boolean allow(String iResource, int iCRUDOperation) {
    final String specificResource = ORule.mapLegacyResourceToSpecificResource(iResource);
    final ORule.ResourceGeneric resourceGeneric = ORule.mapLegacyResourceToGenericResource(iResource);

    if (specificResource == null || specificResource.equals("*"))
      return allow(resourceGeneric, null, iCRUDOperation);

    return allow(resourceGeneric, specificResource, iCRUDOperation);
  }

  @Deprecated
  @Override
  public boolean hasRule(String iResource) {
    final String specificResource = ORule.mapLegacyResourceToSpecificResource(iResource);
    final ORule.ResourceGeneric resourceGeneric = ORule.mapLegacyResourceToGenericResource(iResource);

    if (specificResource == null || specificResource.equals("*"))
      return hasRule(resourceGeneric, null);

    return hasRule(resourceGeneric, specificResource);
  }

  @Override
  public OSecurityRole addRule(String iResource, int iOperation) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OSecurityRole grant(String iResource, int iOperation) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OSecurityRole revoke(String iResource, int iOperation) {
    throw new UnsupportedOperationException();
  }

  public String getName() {
    return name;
  }

  public ALLOW_MODES getMode() {
    return mode;
  }

  public ORole setMode(final ALLOW_MODES iMode) {
    throw new UnsupportedOperationException();
  }

  public OSecurityRole getParentRole() {
    return parentRole;
  }

  public ORole setParentRole(final OSecurityRole iParent) {
    throw new UnsupportedOperationException();
  }

  public Set<ORule> getRuleSet() {
    return new HashSet<ORule>(rules.values());
  }

  @Override
  public String toString() {
    return getName();
  }

  @Override
  public OIdentifiable getIdentity() {
    return rid;
  }

  @Override
  public ODocument getDocument() {
    return role.getDocument();
  }
}
