package com.orientechnologies.orient.core.metadata.security;

import com.orientechnologies.orient.core.db.record.OIdentifiable;

import java.util.Map;

/**
 * @author Andrey Lomakin <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 03/11/14
 */
public interface OSecurityRole {
	public enum ALLOW_MODES {
		DENY_ALL_BUT, ALLOW_ALL_BUT
	}

	public boolean allow(final String iResource, final int iCRUDOperation);

	public boolean hasRule(final String iResource);

	public OSecurityRole addRule(final String iResource, final int iOperation);

	public OSecurityRole grant(final String iResource, final int iOperation);

	public OSecurityRole revoke(final String iResource, final int iOperation);

	public String getName();

	public OSecurityRole.ALLOW_MODES getMode();

	public OSecurityRole setMode(final OSecurityRole.ALLOW_MODES iMode);

	public OSecurityRole getParentRole();

	public OSecurityRole setParentRole(final OSecurityRole iParent);

	public Map<String, Byte> getRules();

	public OIdentifiable getIdentity();
}
