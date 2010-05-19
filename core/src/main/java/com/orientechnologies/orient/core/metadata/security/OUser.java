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
import java.util.List;
import java.util.Set;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.exception.OSecurityAccessException;
import com.orientechnologies.orient.core.metadata.schema.OMetadataRecord;
import com.orientechnologies.orient.core.metadata.security.ORole.OPERATIONS;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.security.OSecurityManager;

/**
 * Contains the user settings about security and permissions. Each user has one or more roles associated. Roles contains the
 * permission rules that define what the user can access and what he can't.
 * 
 * @author Luca Garulli
 * 
 * @see ORole
 */
public class OUser extends OMetadataRecord {
	protected String			name;
	protected String			password;
	protected Set<ORole>	roles	= new HashSet<ORole>();

	/**
	 * Constructor used in unmarshalling.
	 */
	public OUser(final ODatabaseRecord<?> iDatabase) {
		super(iDatabase, "OUser");
	}

	public OUser(final ODatabaseRecord<?> iDatabase, final String iName) {
		this(iDatabase);
		name = iName;
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
	public ORole allow(final String iResource, final OPERATIONS iOperation) {
		if (roles == null || roles.isEmpty())
			throw new OSecurityAccessException("User '" + name + "' has no role defined");

		final ORole role = checkIfAllowed(iResource, iOperation);

		if (role == null)
			throw new OSecurityAccessException("User '" + name + "' has no the permission to execute the operation '" + iOperation
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
	public ORole checkIfAllowed(final String iResource, final OPERATIONS iOperation) {
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

	public boolean checkPassword(String iPassword) {
		return OSecurityManager.instance().check(iPassword, password);
	}

	public String getName() {
		return this.name;
	}

	public String getPassword() {
		return password;
	}

	public OUser setPassword(final String iPassword) {
		this.password = OSecurityManager.instance().digest2String(iPassword);
		return this;
	}

	public void setPasswordEncoded(String iPassword) {
		this.password = iPassword;
	}

	public Set<ORole> getRoles() {
		return roles;
	}

	public OUser addRole(final String iRole) {
		if (iRole != null)
			addRole(database.getMetadata().getSecurity().getRole(iRole));
		return this;
	}

	public OUser addRole(final ORole iRole) {
		if (iRole != null)
			roles.add(iRole);

		return this;
	}

	public OUser fromDocument(final ODocument iSource) {
		name = iSource.field("name");
		password = iSource.field("password");

		ORole role;
		List<String> storedRoles = iSource.field("roles");
		for (String r : storedRoles) {
			role = database.getMetadata().getSecurity().getRole(r);
			roles.add(role);
		}
		return this;
	}

	public byte[] toStream() {
		field("name", name);
		field("password", password);

		Set<String> storedRoles = new HashSet<String>();
		for (ORole r : roles) {
			storedRoles.add(r.getName());
		}

		field("roles", storedRoles);
		return super.toStream();
	}

	@Override
	public String toString() {
		return name;
	}
}
