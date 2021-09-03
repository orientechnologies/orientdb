package com.orientechnologies.orient.core.metadata.security;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OSecurityAccessException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.security.OSecurityManager;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 03/11/14
 */
public class OImmutableUser implements OSecurityUser {
  private static final long serialVersionUID = 1L;
  private final long version;

  private final String name;
  private final String password;

  private final Set<OImmutableRole> roles = new HashSet<OImmutableRole>();

  private final STATUSES status;
  private final ORID rid;
  private final String userType;

  public OImmutableUser(long version, OSecurityUser user) {
    this.version = version;
    this.name = user.getName();
    this.password = user.getPassword();
    this.status = user.getAccountStatus();
    this.rid = user.getIdentity().getIdentity();
    this.userType = user.getUserType();

    for (OSecurityRole role : user.getRoles()) {
      roles.add(new OImmutableRole(role));
    }
  }

  public OImmutableUser(String name, String userType) {
    this(name, "", userType, null);
  }

  public OImmutableUser(String name, String password, String userType, OSecurityRole role) {
    this.version = 0;
    this.name = name;
    this.password = password;
    this.status = STATUSES.ACTIVE;
    this.rid = new ORecordId(-1, -1);
    this.userType = userType;
    if (role != null) {
      OImmutableRole immutableRole;
      if (role instanceof OImmutableRole) {
        immutableRole = (OImmutableRole) role;
      } else {
        immutableRole = new OImmutableRole(role);
      }
      roles.add(immutableRole);
    }
  }

  public OSecurityRole allow(
      final ORule.ResourceGeneric resourceGeneric,
      final String resourceSpecific,
      final int iOperation) {
    if (roles.isEmpty())
      throw new OSecurityAccessException(getName(), "User '" + getName() + "' has no role defined");

    final OSecurityRole role = checkIfAllowed(resourceGeneric, resourceSpecific, iOperation);

    if (role == null)
      throw new OSecurityAccessException(
          getName(),
          "User '"
              + getName()
              + "' does not have permission to execute the operation '"
              + ORole.permissionToString(iOperation)
              + "' against the resource: "
              + resourceGeneric
              + "."
              + resourceSpecific);

    return role;
  }

  public OSecurityRole checkIfAllowed(
      final ORule.ResourceGeneric resourceGeneric,
      final String resourceSpecific,
      final int iOperation) {
    for (OImmutableRole r : roles) {
      if (r == null)
        OLogManager.instance()
            .warn(
                this,
                "User '%s' has a null role, ignoring it.  Consider fixing this user's roles before continuing",
                getName());
      else if (r.allow(resourceGeneric, resourceSpecific, iOperation)) return r;
    }

    return null;
  }

  public boolean isRuleDefined(
      final ORule.ResourceGeneric resourceGeneric, String resourceSpecific) {
    for (OImmutableRole r : roles)
      if (r == null)
        OLogManager.instance()
            .warn(
                this,
                "UseOSecurityAuthenticatorr '%s' has a null role, ignoring it.  Consider fixing this user's roles before continuing",
                getName());
      else if (r.hasRule(resourceGeneric, resourceSpecific)) return true;

    return false;
  }

  @Override
  @Deprecated
  public OSecurityRole allow(String iResource, int iOperation) {
    final String resourceSpecific = ORule.mapLegacyResourceToSpecificResource(iResource);
    final ORule.ResourceGeneric resourceGeneric =
        ORule.mapLegacyResourceToGenericResource(iResource);

    if (resourceSpecific == null || resourceSpecific.equals("*"))
      return allow(resourceGeneric, null, iOperation);

    return allow(resourceGeneric, resourceSpecific, iOperation);
  }

  @Override
  @Deprecated
  public OSecurityRole checkIfAllowed(String iResource, int iOperation) {
    final String resourceSpecific = ORule.mapLegacyResourceToSpecificResource(iResource);
    final ORule.ResourceGeneric resourceGeneric =
        ORule.mapLegacyResourceToGenericResource(iResource);

    if (resourceSpecific == null || resourceSpecific.equals("*"))
      return checkIfAllowed(resourceGeneric, null, iOperation);

    return checkIfAllowed(resourceGeneric, resourceSpecific, iOperation);
  }

  @Override
  @Deprecated
  public boolean isRuleDefined(String iResource) {
    final String resourceSpecific = ORule.mapLegacyResourceToSpecificResource(iResource);
    final ORule.ResourceGeneric resourceGeneric =
        ORule.mapLegacyResourceToGenericResource(iResource);

    if (resourceSpecific == null || resourceSpecific.equals("*"))
      return isRuleDefined(resourceGeneric, null);

    return isRuleDefined(resourceGeneric, resourceSpecific);
  }

  public boolean checkPassword(final String iPassword) {
    return OSecurityManager.checkPassword(iPassword, getPassword());
  }

  public String getName() {
    return name;
  }

  public OUser setName(final String iName) {
    throw new UnsupportedOperationException();
  }

  public String getPassword() {
    return password;
  }

  public OUser setPassword(final String iPassword) {
    throw new UnsupportedOperationException();
  }

  public STATUSES getAccountStatus() {
    return status;
  }

  public void setAccountStatus(STATUSES accountStatus) {
    throw new UnsupportedOperationException();
  }

  public Set<OImmutableRole> getRoles() {
    return Collections.unmodifiableSet(roles);
  }

  public OUser addRole(final String iRole) {
    throw new UnsupportedOperationException();
  }

  public OUser addRole(final OSecurityRole iRole) {
    throw new UnsupportedOperationException();
  }

  public boolean removeRole(final String iRoleName) {
    throw new UnsupportedOperationException();
  }

  public boolean hasRole(final String iRoleName, final boolean iIncludeInherited) {
    for (Iterator<OImmutableRole> it = roles.iterator(); it.hasNext(); ) {
      final OSecurityRole role = it.next();
      if (role.getName().equals(iRoleName)) return true;

      if (iIncludeInherited) {
        OSecurityRole r = role.getParentRole();
        while (r != null) {
          if (r.getName().equals(iRoleName)) return true;
          r = r.getParentRole();
        }
      }
    }

    return false;
  }

  @Override
  public String toString() {
    return getName();
  }

  public long getVersion() {
    return version;
  }

  @Override
  public OIdentifiable getIdentity() {
    return rid;
  }

  @Override
  public String getUserType() {
    return userType;
  }
}
