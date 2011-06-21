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
import com.orientechnologies.orient.core.db.record.OProxedResource;
import com.orientechnologies.orient.core.metadata.security.ORole.ALLOW_MODES;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Manages users and roles.
 * 
 * @author Luca Garulli
 * 
 */
public class OSecurityProxy extends OProxedResource<OSecurityShared> implements OSecurity {
	public OSecurityProxy(final OSecurityShared iDelegate, final ODatabaseRecord iDatabase) {
		super(iDelegate, iDatabase);
	}

	public OUser create() {
		return delegate.create();
	}

	public void load() {
		delegate.load();
	}

	public void close() {
		delegate.close();
	}

	public OUser authenticate(final String iUsername, final String iUserPassword) {
		return delegate.authenticate(iUsername, iUserPassword);
	}

	public OUser getUser(final String iUserName) {
		return delegate.getUser(iUserName);
	}

	public OUser createUser(final String iUserName, final String iUserPassword, final String[] iRoles) {
		return delegate.createUser(iUserName, iUserPassword, iRoles);
	}

	public ORole getRole(final String iRoleName) {
		return delegate.getRole(iRoleName);
	}

	public ORole createRole(final String iRoleName, final ALLOW_MODES iAllowMode) {
		return delegate.createRole(iRoleName, iAllowMode);
	}

	public ORole createRole(final String iRoleName, final ORole iParent, final ALLOW_MODES iAllowMode) {
		return delegate.createRole(iRoleName, iParent, iAllowMode);
	}

	public List<ODocument> getUsers() {
		return delegate.getUsers();
	}

	public List<ODocument> getRoles() {
		return delegate.getRoles();
	}

	public String toString() {
		return delegate.toString();
	}
}
