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

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.exception.OSecurityException;
import com.orientechnologies.orient.core.metadata.schema.OMetadataRecord;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

public class OSecurity extends OMetadataRecord {
	protected Map<String, ORole>	roles	= new LinkedHashMap<String, ORole>();
	protected Map<String, OUser>	users	= new LinkedHashMap<String, OUser>();

	public OSecurity(final ODatabaseRecord<?> iDatabaseOwner, final int schemaClusterId) {
		super(iDatabaseOwner);
	}

	public OUser getUser(final String iUserName) {
		return users.get(iUserName);
	}

	public OUser createUser(final String iUserName, final String iUserPassword, final String[] iRoles) {
		String key = iUserName.toLowerCase();

		if (users.containsKey(key))
			throw new OSecurityException("User " + iUserName + " already exists in current database");

		OUser user = new OUser(database, iUserName);
		users.put(iUserName.toLowerCase(), user);

		user.setDatabase(database);
		user.setPassword(iUserPassword);

		if (iRoles != null)
			for (String r : iRoles) {
				user.addRole(r);
			}

		setDirty();

		return user;
	}

	public ORole getRole(final String iRoleName) {
		return roles.get(iRoleName);
	}

	public ORole createRole(final String iRoleName, final ORole.ALLOW_MODES iAllowMode) {
		return createRole(iRoleName, null, iAllowMode);
	}

	public ORole createRole(final String iRoleName, final ORole iParent, final ORole.ALLOW_MODES iAllowMode) {
		final ORole role = new ORole(database, iRoleName, iParent, iAllowMode);
		return createRole(role);
	}

	public ORole createRole(final ORole iRole) {
		if (roles.containsKey(iRole.name))
			throw new OConfigurationException("Role " + iRole.name + " is already defined");

		roles.put(iRole.name, iRole);

		setDirty();

		return iRole;
	}

	@Override
	public OSecurity fromStream(final byte[] iBuffer) {
		super.fromStream(iBuffer);

		ORole role;
		List<ODocument> storedRoles = field("roles");
		for (ODocument r : storedRoles) {
			role = new ORole(database).fromDocument(r);
			roles.put(role.name, role);
		}

		OUser user;
		List<ODocument> storedUsers = field("users");
		for (ODocument u : storedUsers) {
			user = new OUser(database).fromDocument(u);
			users.put(user.name, user);
		}

		return this;
	}

	@Override
	public byte[] toStream() {
		field("roles", roles.values(), OType.LINKSET);
		field("users", users.values(), OType.LINKSET);
		return super.toStream();
	}

	public Collection<OUser> getUsers() {
		return Collections.unmodifiableCollection(users.values());
	}

	public Collection<ORole> getRoles() {
		return Collections.unmodifiableCollection(roles.values());
	}

	@Override
	public OSecurity load() {
		recordId.fromString(database.getStorage().getConfiguration().securityRecordId);
		return (OSecurity) super.load();
	}
}
