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
	public enum STATUSES {
		SUSPENDED, ACTIVE
	}

  public OSecurityRole allow(final ORule.ResourceGeneric resourceGeneric, String resourceSpecific, final int iOperation);

  public OSecurityRole checkIfAllowed(final ORule.ResourceGeneric resourceGeneric, String resourceSpecific, final int iOperation);

  public boolean isRuleDefined(final ORule.ResourceGeneric resourceGeneric, String resourceSpecific);

	@Deprecated
	public OSecurityRole allow(final String iResource, final int iOperation);

	@Deprecated
	public OSecurityRole checkIfAllowed(final String iResource, final int iOperation);

	@Deprecated
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
  
  public ODocument getDocument();
}
