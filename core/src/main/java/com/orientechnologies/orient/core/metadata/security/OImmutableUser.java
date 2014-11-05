package com.orientechnologies.orient.core.metadata.security;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OSecurityAccessException;
import com.orientechnologies.orient.core.exception.OSecurityException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.security.OSecurityManager;

import java.util.*;

/**
 * @author Andrey Lomakin <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 03/11/14
 */
public class OImmutableUser implements OSecurityUser {
  private final long                version;

  private final String              name;
  private final String              password;

  private final Set<OImmutableRole> roles = new HashSet<OImmutableRole>();

  private final STATUSES            status;
  private final ORID                rid;

  public OImmutableUser(long version, OUser user) {
    this.version = version;
    this.name = user.getName();
    this.password = user.getPassword();
    this.status = user.getAccountStatus();
    this.rid = user.getIdentity().getIdentity();

    for (ORole role : user.getRoles()) {
      roles.add(new OImmutableRole(role));
    }
  }

  public OSecurityRole allow(final String iResource, final int iOperation) {
    if (roles.isEmpty())
      throw new OSecurityAccessException(getName(), "User '" + getName() + "' has no role defined");

    final OSecurityRole role = checkIfAllowed(iResource, iOperation);

    if (role == null)
      throw new OSecurityAccessException(getName(), "User '" + getName() + "' has no the permission to execute the operation '"
          + ORole.permissionToString(iOperation) + "' against the resource: " + iResource);

    return role;
  }

  public OSecurityRole checkIfAllowed(final String iResource, final int iOperation) {
    for (OImmutableRole r : roles) {
      if (r == null)
        OLogManager.instance().warn(this,
            "User '%s' has a null role, bypass it. Consider to fix this user roles before to continue", getName());
      else if (r.allow(iResource, iOperation))
        return r;
    }

    return null;
  }

  public boolean isRuleDefined(final String iResource) {
    for (OImmutableRole r : roles)
      if (r == null)
        OLogManager.instance().warn(this,
            "User '%s' has a null role, bypass it. Consider to fix this user roles before to continue", getName());
      else if (r.hasRule(iResource))
        return true;

    return false;
  }

  public boolean checkPassword(final String iPassword) {
    return OSecurityManager.instance().check(iPassword, getPassword());
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
    for (Iterator<OImmutableRole> it = roles.iterator(); it.hasNext();) {
      final OSecurityRole role = it.next();
      if (role.getName().equals(iRoleName))
        return true;

      if (iIncludeInherited) {
        OSecurityRole r = role.getParentRole();
        while (r != null) {
          if (r.getName().equals(iRoleName))
            return true;
          r = role.getParentRole();
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
}