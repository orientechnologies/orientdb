package com.orientechnologies.orient.core.metadata.security;

import com.orientechnologies.orient.core.db.record.OIdentifiable;

import java.util.Set;

/**
 * @author Andrey Lomakin <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 03/11/14
 */
public interface OSecurityUser {
	public enum STATUSES {
		SUSPENDED, ACTIVE
	}

  public OSecurityRole allow(final String iResource, final int iOperation);

  public OSecurityRole checkIfAllowed(final String iResource, final int iOperation);

  public boolean isRuleDefined(final String iResource);

  public boolean checkPassword(final String iPassword);

  public String getName();

  public OSecurityUser setName(final String iName);

  public String getPassword();

  public OSecurityUser setPassword(final String iPassword);

  public OSecurityUser.STATUSES getAccountStatus();

  public void setAccountStatus(OSecurityUser.STATUSES accountStatus);

  public Set<? extends OSecurityRole> getRoles();

  public OSecurityUser addRole(final String iRole);

  public OSecurityUser addRole(final OSecurityRole iRole);

  public boolean removeRole(final String iRoleName);

  public boolean hasRole(final String iRoleName, final boolean iIncludeInherited);

	public OIdentifiable getIdentity();
}
