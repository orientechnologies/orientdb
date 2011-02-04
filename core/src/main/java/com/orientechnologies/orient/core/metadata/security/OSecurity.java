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

import java.util.List;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

/**
 * Manages users and roles.
 * 
 * @author Luca Garulli
 * 
 */
public class OSecurity {
	private ODatabaseRecord	database;

	public OSecurity(final ODatabaseRecord iDatabaseOwner) {
		database = iDatabaseOwner;
	}

	public OUser getUser(final String iUserName) {
		final List<ODocument> result = database.command(
				new OSQLSynchQuery<ODocument>("select from OUser where name = '" + iUserName + "'").setFetchPlan("*:-1")).execute();

		if (result != null && result.size() > 0)
			return new OUser(result.get(0));

		return null;
	}

	public OUser createUser(final String iUserName, final String iUserPassword, final String[] iRoles) {
		final OUser user = new OUser(database, iUserName, iUserPassword);

		if (iRoles != null)
			for (String r : iRoles) {
				user.addRole(r);
			}

		return user.save();
	}

	public ORole getRole(final String iRoleName) {
		final List<ODocument> result = database.command(
				new OSQLSynchQuery<ODocument>("select from ORole where name = '" + iRoleName + "'").setFetchPlan("*:-1")).execute();

		if (result != null && result.size() > 0)
			return new ORole(result.get(0));

		return null;
	}

	public ORole createRole(final String iRoleName, final ORole.ALLOW_MODES iAllowMode) {
		return createRole(iRoleName, null, iAllowMode);
	}

	public ORole createRole(final String iRoleName, final ORole iParent, final ORole.ALLOW_MODES iAllowMode) {
		final ORole role = new ORole(database, iRoleName, iParent, iAllowMode);
		return createRole(role);
	}

	public ORole createRole(final ORole iRole) {
		return iRole.save();
	}

	public List<ODocument> getUsers() {
		return database.command(new OSQLSynchQuery<ODocument>("select from OUser")).execute();
	}

	public List<ODocument> getRoles() {
		return database.command(new OSQLSynchQuery<ODocument>("select from ORole")).execute();
	}
}
