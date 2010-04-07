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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.exception.OSecurityException;
import com.orientechnologies.orient.core.metadata.security.OUser.MODE;
import com.orientechnologies.orient.core.record.ORecordAbstract;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import com.orientechnologies.orient.core.record.impl.ORecordColumn;

public class OSecurity extends ORecordBytes {
	protected Map<String, OUser>	users								= new LinkedHashMap<String, OUser>();

	public static final int				SECURITY_RECORD_NUM	= 2;

	public OSecurity(ODatabaseRecord<?> iDatabaseOwner, int schemaClusterId) {
		super(iDatabaseOwner);
	}

	public OUser getUser(String iUserName) {
		return users.get(iUserName);
	}

	public OUser createUser(String iUserName, String iUserPassword, MODE iMode, OUser iParentUser) {
		String key = iUserName.toLowerCase();

		if (users.containsKey(key))
			throw new OSecurityException("User " + iUserName + " already exists in current database");

		OUser user = new OUser(iUserName);
		user.password(iUserPassword);
		user.mode(iMode);
		user.inherit(iParentUser);

		users.put(iUserName.toLowerCase(), user);

		setDirty();

		return user;
	}

	public OSecurity fromStream(byte[] buffer) {
		ORecordColumn record = new ORecordColumn(database, buffer);

		Map<OUser, String> userInheritance = new HashMap<OUser, String>();

		// REGISTER ALL USERS
		OUser user;
		int usersNum = Integer.parseInt(record.next());
		int aclNum;
		String inheritUser;
		for (int u = 0; u < usersNum; ++u) {
			user = new OUser(record.next());
			users.put(user.name.toLowerCase(), user);

			// SERIALIZE PASSWORD ONLY IN STORAGE MODE (AKA TO THE STORAGE)
			user.passwordEncoded(record.next());

			user.internalMode((byte) record.next().charAt(0));

			inheritUser = record.next();

			if (inheritUser != null && inheritUser.length() > 0)
				// SAVE THE INHERITANCE TO REBUILD DEPENDENCIES AFTER THE LOAD OF ALL USERS
				userInheritance.put(user, inheritUser);

			// REGISTER ALL ACL
			aclNum = Integer.parseInt(record.next());
			for (int a = 0; a < aclNum; ++a) {
				user.acl.put(record.next(), record.next().getBytes());
			}
		}

		// REBUILD THE USER DEPENDENCIES
		for (Entry<OUser, String> entry : userInheritance.entrySet()) {
			entry.getKey().inherit(users.get(entry.getValue().toLowerCase()));
		}

		return this;
	}

	public byte[] toStream() {
		ORecordColumn record = new ORecordColumn(database);

		// WRITE USERS
		record.add(String.valueOf(users.size()));
		for (OUser user : users.values()) {
			record.add(user.name());
			if (user.password() != null)
				// SERIALIZE PASSWORD ONLY IN STORAGE MODE (AKA TO THE STORAGE)
				record.add("'" + user.password() + "'");
			else
				record.add("");

			record.add(String.valueOf((char) user.internalMode()));
			record.add(user.inherit() != null ? user.inherit().name : "");

			// WRITE ACL
			record.add(String.valueOf(user.acl.size()));
			for (Entry<String, byte[]> aclEntry : user.acl.entrySet()) {
				record.add(aclEntry.getKey());
				record.add(String.valueOf(aclEntry.getValue()));
			}
		}
		return record.toStream();
	}

	public Collection<OUser> getUsers() {
		return Collections.unmodifiableCollection(users.values());
	}

	public ORecordAbstract<byte[]> load(int schemaClusterId) {
		setIdentity(schemaClusterId, SECURITY_RECORD_NUM);
		return super.load();
	}
}
