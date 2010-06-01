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
import com.orientechnologies.orient.core.query.nativ.ONativeSynchQuery;
import com.orientechnologies.orient.core.query.nativ.OQueryContextNativeSchema;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Manages users and roles.
 * 
 * @author Luca Garulli
 * 
 */
@SuppressWarnings("unchecked")
public class OSecurity {
	private ODatabaseRecord<?>	database;

	public OSecurity(final ODatabaseRecord<?> iDatabaseOwner) {
		database = iDatabaseOwner;
	}

	public OUser getUser(final String iUserName) {
		ODocument result = new ONativeSynchQuery<ODocument, OQueryContextNativeSchema<ODocument>>(
				(ODatabaseRecord<ODocument>) database, OUser.class.getSimpleName(), new OQueryContextNativeSchema<ODocument>()) {

			@Override
			public boolean filter(OQueryContextNativeSchema<ODocument> iRecord) {
				return iRecord.field("name").eq(iUserName).go();
			};

		}.executeFirst();

		if (result != null)
			return new OUser(result);

		return null;
	}

	public OUser createUser(final String iUserName, final String iUserPassword, final String[] iRoles) {
		OUser user = new OUser(database, iUserName);
		user.setPassword(iUserPassword);

		if (iRoles != null)
			for (String r : iRoles) {
				user.addRole(r);
			}

		return user.save();
	}

	public ORole getRole(final String iRoleName) {
		ODocument result = new ONativeSynchQuery<ODocument, OQueryContextNativeSchema<ODocument>>(
				(ODatabaseRecord<ODocument>) database, ORole.class.getSimpleName(), new OQueryContextNativeSchema<ODocument>()) {

			@Override
			public boolean filter(OQueryContextNativeSchema<ODocument> iRecord) {
				return iRecord.field("name").eq(iRoleName).go();
			};

		}.executeFirst();

		if (result != null)
			return new ORole(result);

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
		return new ONativeSynchQuery<ODocument, OQueryContextNativeSchema<ODocument>>((ODatabaseRecord<ODocument>) database,
				OUser.class.getSimpleName(), new OQueryContextNativeSchema<ODocument>()) {

			@Override
			public boolean filter(OQueryContextNativeSchema<ODocument> iRecord) {
				return true;
			};

		}.execute();
	}

	public List<ODocument> getRoles() {
		return new ONativeSynchQuery<ODocument, OQueryContextNativeSchema<ODocument>>((ODatabaseRecord<ODocument>) database,
				ORole.class.getSimpleName(), new OQueryContextNativeSchema<ODocument>()) {

			@Override
			public boolean filter(OQueryContextNativeSchema<ODocument> iRecord) {
				return true;
			};

		}.execute();
	}
}
