/*
 * Copyright 1999-2010 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.metadata.security;

import java.util.HashSet;
import java.util.Set;

import com.orientechnologies.orient.core.annotation.OAfterDeserialization;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.exception.OSecurityAccessException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.security.OSecurityManager;
import com.orientechnologies.orient.core.type.ODocumentWrapper;

/**
 * Contains the user settings about security and permissions. Each user has one or more roles associated. Roles contains the
 * permission rules that define what the user can access and what he can't.
 * 
 * @author Luca Garulli
 * 
 * @see ORole
 */
public class OUser extends ODocumentWrapper {
	public static final String	ADMIN	= "admin";

	public enum STATUSES {
		SUSPENDED, ACTIVE
	}

	// AVOID THE INVOCATION OF SETTER
	protected Set<ORole>	roles	= new HashSet<ORole>();

	/**
	 * Constructor used in unmarshalling.
	 */
	public OUser() {
	}

	public OUser(final ODatabaseRecord iDatabase, final String iName) {
		super(iDatabase, "OUser");
		document.field("name", iName);
		setAccountStatus(STATUSES.ACTIVE);
	}

	public OUser(ODatabaseRecord iDatabase, String iUserName, final String iUserPassword) {
		super(iDatabase, "OUser");
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

	@Override
	@OAfterDeserialization
	public void fromStream(final ODocument iSource) {
		if (document != null)
			return;

		document = iSource;

		roles = new HashSet<ORole>();
		final Set<ODocument> loadedRoles = iSource.field("roles");
		if (loadedRoles != null)
			for (ODocument d : loadedRoles) {
				roles.add(document.getDatabase().getMetadata().getSecurity().getRole((String) d.field("name")));
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
		if (roles == null || roles.isEmpty())
			throw new OSecurityAccessException(document.getDatabase().getName(), "User '" + document.field("name")
					+ "' has no role defined");

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
		for (ORole r : roles)
			if (r.allow(iResource, iOperation))
				return r;

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
			if (r.hasRule(iResource))
				return true;

		return false;
	}

	public boolean checkPassword(final String iPassword) {
		return OSecurityManager.instance().check(iPassword, (String) document.field("password"));
	}

	public String getName() {
		return document.field("name");
	}

	public String getPassword() {
		return document.field("password");
	}

	public OUser setPassword(final String iPassword) {
		document.field("password", iPassword);
		return this;
	}

	public static final String encryptPassword(final String iPassword) {
		return OSecurityManager.instance().digest2String(iPassword);
	}

	public STATUSES getAccountStatus() {
		return STATUSES.valueOf((String) document.field("status"));
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

	public OUser addRole(final ORole iRole) {
		if (iRole != null)
			roles.add(iRole);

		final HashSet<ODocument> persistentRoles = new HashSet<ODocument>();
		for (ORole r : roles) {
			persistentRoles.add(r.toStream());
		}
		document.field("roles", persistentRoles);
		return this;
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
}
