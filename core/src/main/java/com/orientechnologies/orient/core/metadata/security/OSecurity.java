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
import java.util.Map;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.exception.OSecurityException;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;

public class OSecurity {
	private ODatabaseRecord<?>		database;
	protected Map<String, ORole>	roles	= new LinkedHashMap<String, ORole>();
	protected Map<String, OUser>	users	= new LinkedHashMap<String, OUser>();

	public OSecurity(final ODatabaseRecord<?> iDatabaseOwner) {
		database = iDatabaseOwner;
	}

	public void load() {
		if (database.getClusterIdByName(ORole.class.getSimpleName()) == -1)
			return;

		ODocument doc;
		for (ORecord<?> rec : database.browseCluster(ORole.class.getSimpleName())) {
			doc = (ODocument) rec;
			roles.put((String) doc.field("name"), new ORole(database).fromDocument(doc));
		}

		for (ORecord<?> rec : database.browseCluster(OUser.class.getSimpleName())) {
			doc = (ODocument) rec;
			users.put((String) doc.field("name"), new OUser(database).fromDocument(doc));
		}
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

		return user.save();
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

		return iRole.save();
	}

	public Collection<OUser> getUsers() {
		return Collections.unmodifiableCollection(users.values());
	}

	public Collection<ORole> getRoles() {
		return Collections.unmodifiableCollection(roles.values());
	}
}
