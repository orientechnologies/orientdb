/*
  *
  *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
  *  *
  *  *  Licensed under the Apache License, Version 2.0 (the "License");
  *  *  you may not use this file except in compliance with the License.
  *  *  You may obtain a copy of the License at
  *  *
  *  *       http://www.apache.org/licenses/LICENSE-2.0
  *  *
  *  *  Unless required by applicable law or agreed to in writing, software
  *  *  distributed under the License is distributed on an "AS IS" BASIS,
  *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  *  *  See the License for the specific language governing permissions and
  *  *  limitations under the License.
  *  *
  *  * For more information: http://www.orientechnologies.com
  *
  */
package com.orientechnologies.orient.core.metadata.security;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.annotation.OAfterDeserialization;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OSecurityAccessException;
import com.orientechnologies.orient.core.exception.OSecurityException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.security.OSecurityManager;
import com.orientechnologies.orient.core.type.ODocumentWrapper;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Contains the user settings about security and permissions. Each user has one or more roles associated. Roles contains the
 * permission rules that define what the user can access and what he cannot.
 * 
 * @author Luca Garulli
 * 
 * @see ORole
 */
public class OUser extends ODocumentWrapper implements  OSecurityUser {
  public static final String ADMIN            = "admin";
  public static final String CLASS_NAME       = "OUser";
  private static final long  serialVersionUID = 1L;
  // AVOID THE INVOCATION OF SETTER
  protected Set<ORole>       roles            = new HashSet<ORole>();


  /**
   * Constructor used in unmarshalling.
   */
  public OUser() {
  }

  public OUser(final String iName) {
    super(CLASS_NAME);
    document.field("name", iName);
    setAccountStatus(STATUSES.ACTIVE);
  }

  public OUser(String iUserName, final String iUserPassword) {
    super("OUser");
    document.field("name", iUserName);
    setPassword(iUserPassword);
    setAccountStatus(STATUSES.ACTIVE);
  }

  /**
   * Create the user by reading the source document.
   */
  public OUser(final ODocument iSource) {
    fromStream(iSource);
  }

  public static final String encryptPassword(final String iPassword) {
    return OSecurityManager.instance().digest2String(iPassword, true);
  }

  @Override
  @OAfterDeserialization
  public void fromStream(final ODocument iSource) {
    if (document != null)
      return;

    document = iSource;

    roles = new HashSet<ORole>();
    final Collection<ODocument> loadedRoles = iSource.field("roles");
    if (loadedRoles != null)
      for (final ODocument d : loadedRoles) {
        final ORole role = document.getDatabase().getMetadata().getSecurity().getRole((String) d.field("name"));
        if (role == null) {
          OLogManager.instance().warn(this, "User '%s' declare to have the role '%s' but it does not exist in database, skipt it",
              getName(), d.field("name"));
        } else
          roles.add(role);
      }
  }

  /**
   * Checks if the user has the permission to access to the requested resource for the requested operation.
   * 
   * @param iResource
   *          Requested resource
   * @param iOperation
   *          Requested operation
   * @return The role that has granted the permission if any, otherwise a OSecurityAccessException exception is raised
   * @exception OSecurityAccessException
   */
  public ORole allow(final String iResource, final int iOperation) {
    if (roles == null || roles.isEmpty()) {
      if (document.field("roles") != null && !((Collection<OIdentifiable>) document.field("roles")).isEmpty()) {
        final ODocument doc = document;
        document = null;
        fromStream(doc);
      } else
        throw new OSecurityAccessException(document.getDatabase().getName(), "User '" + document.field("name")
            + "' has no role defined");
    }

    final ORole role = checkIfAllowed(iResource, iOperation);

    if (role == null)
      throw new OSecurityAccessException(document.getDatabase().getName(), "User '" + document.field("name")
          + "' has no the permission to execute the operation '" + ORole.permissionToString(iOperation)
          + "' against the resource: " + iResource);

    return role;
  }

  /**
   * Checks if the user has the permission to access to the requested resource for the requested operation.
   * 
   * @param iResource
   *          Requested resource
   * @param iOperation
   *          Requested operation
   * @return The role that has granted the permission if any, otherwise null
   */
  public ORole checkIfAllowed(final String iResource, final int iOperation) {
    for (ORole r : roles) {
      if (r == null)
        OLogManager.instance().warn(this,
            "User '%s' has a null role, bypass it. Consider to fix this user roles before to continue", getName());
      else if (r.allow(iResource, iOperation))
        return r;
    }

    return null;
  }

  /**
   * Checks if a rule was defined for the user.
   * 
   * @param iResource
   *          Requested resource
   * @return True is a rule is defined, otherwise false
   */
  public boolean isRuleDefined(final String iResource) {
    for (ORole r : roles)
      if (r == null)
        OLogManager.instance().warn(this,
            "User '%s' has a null role, bypass it. Consider to fix this user roles before to continue", getName());
      else if (r.hasRule(iResource))
        return true;

    return false;
  }

  public boolean checkPassword(final String iPassword) {
    return OSecurityManager.instance().check(iPassword, (String) document.field("password"));
  }

  public String getName() {
    return document.field("name");
  }

  public OUser setName(final String iName) {
    document.field("name", iName);
    return this;
  }

  public String getPassword() {
    return document.field("password");
  }

  public OUser setPassword(final String iPassword) {
    document.field("password", iPassword);
    return this;
  }

  public STATUSES getAccountStatus() {
    final String status = (String) document.field("status");
    if (status == null)
      throw new OSecurityException("User '" + getName() + "' has no status");
    return STATUSES.valueOf(status);
  }

  public void setAccountStatus(STATUSES accountStatus) {
    document.field("status", accountStatus);
  }

  public Set<ORole> getRoles() {
    return roles;
  }

  public OUser addRole(final String iRole) {
    if (iRole != null)
      addRole(document.getDatabase().getMetadata().getSecurity().getRole(iRole));
    return this;
  }

  public OUser addRole(final OSecurityRole iRole) {
    if (iRole != null)
      roles.add((ORole)iRole);

    final HashSet<ODocument> persistentRoles = new HashSet<ODocument>();
    for (ORole r : roles) {
      persistentRoles.add(r.toStream());
    }
    document.field("roles", persistentRoles);
    return this;
  }

  public boolean removeRole(final String iRoleName) {
    for (Iterator<ORole> it = roles.iterator(); it.hasNext();)
      if (it.next().getName().equals(iRoleName)) {
        it.remove();
        return true;
      }
    return false;
  }

  public boolean hasRole(final String iRoleName, final boolean iIncludeInherited) {
    for (Iterator<ORole> it = roles.iterator(); it.hasNext();) {
      final ORole role = it.next();
      if (role.getName().equals(iRoleName))
        return true;

      if (iIncludeInherited) {
        ORole r = role.getParentRole();
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
  @SuppressWarnings("unchecked")
  public OUser save() {
    document.save(OUser.class.getSimpleName());
    return this;
  }

  @Override
  public String toString() {
    return getName();
  }

	@Override
	public OIdentifiable getIdentity() {
		return document;
	}
}
