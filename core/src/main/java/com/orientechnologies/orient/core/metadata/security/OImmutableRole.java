package com.orientechnologies.orient.core.metadata.security;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author Andrey Lomakin <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 03/11/14
 */
public class OImmutableRole implements OSecurityRole {
  private final ALLOW_MODES       mode;
  private final OSecurityRole     parentRole;
  private final Map<String, Byte> rules = new HashMap<String, Byte>();
  private final String            name;
  private final ORID              rid;
  private final ORole             role;

  public OImmutableRole(ORole role) {
    if (role.getParentRole() == null)
      this.parentRole = null;
    else
      this.parentRole = new OImmutableRole(role.getParentRole());

    this.mode = role.getMode();
    this.name = role.getName();
    this.rid = role.getIdentity().getIdentity();
    this.role = role;
    rules.putAll(role.getRules());
  }

  public boolean allow(final String iResource, final int iCRUDOperation) {
    // CHECK FOR SECURITY AS DIRECT RESOURCE
    final Byte access = rules.get(iResource.toLowerCase());
    if (access != null) {
      final byte mask = (byte) iCRUDOperation;

      return (access & mask) == mask;
    } else if (parentRole != null)
      // DELEGATE TO THE PARENT ROLE IF ANY
      return parentRole.allow(iResource, iCRUDOperation);

    return mode == ALLOW_MODES.ALLOW_ALL_BUT;
  }

  public boolean hasRule(final String iResource) {
    return rules.containsKey(iResource.toLowerCase());
  }

  public OSecurityRole addRule(final String iResource, final int iOperation) {
    throw new UnsupportedOperationException();
  }

  public OSecurityRole grant(final String iResource, final int iOperation) {
    throw new UnsupportedOperationException();
  }

  public ORole revoke(final String iResource, final int iOperation) {
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

  public Map<String, Byte> getRules() {
    return Collections.unmodifiableMap(rules);
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
