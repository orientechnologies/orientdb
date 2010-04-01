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

import java.util.LinkedHashMap;
import java.util.Map;

import com.orientechnologies.orient.core.exception.OSecurityAccessException;
import com.orientechnologies.orient.core.security.OSecurityManager;

/**
 * Mode = ALLOW (allow all but) or DENY (deny all but)
 */
public class OUser {
	public enum MODE {
		ALLOW_ALL_BUT, DENY_ALL_BUT
	}

	// CRUD OPERATIONS
	public final static int				CREATE						= 1;
	public final static int				READ							= 2;
	public final static int				UPDATE						= 4;
	public final static int				DELETE						= 8;

	public final static String		ALL								= "*";
	public final static String		DATABASE					= "database";
	public final static String		CLUSTER						= "database.cluster";
	public final static String		CLASS							= "database.class";
	public final static String		ALL_CLASSES				= "database.class.*";
	public static final String		QUERY							= "database.query";
	public final static String		SERVER_ADMIN			= "server.admin";

	protected final static int		ACL_OPERATION_NUM	= 4;

	protected final static byte		DENY							= '0';
	protected final static byte		ALLOW							= '1';

	protected String							name;
	protected String							password;
	protected byte								mode							= DENY;
	protected OUser								inherit;
	protected Map<String, byte[]>	acl								= new LinkedHashMap<String, byte[]>();

	public OUser(String iName) {
		name = iName;
	}

	public boolean checkPassword(String iPassword) {
		return OSecurityManager.instance().check(iPassword, password);
	}

	public boolean allow(String iResource, int iCRUDOperation) {
		if (iCRUDOperation >= ACL_OPERATION_NUM)
			throw new OSecurityAccessException("Requested invalid operation '" + iCRUDOperation + "' against resource: " + iResource);

		// CHECK FOR SECURITY AS DIRECT RESOURCE
		byte[] access = acl.get(iResource);
		if (access != null)
			return access[iCRUDOperation] != mode;

		// CHECK FOR SECURITY IN DERIVED RESOURCES, IF ANY
		if (iResource.startsWith(CLASS)) {
			// CHECK FOR SECURITY IN "ALL CLASSES"
			access = acl.get(ALL_CLASSES);
			if (access != null)
				return access[iCRUDOperation] != mode;
		}

		return mode == ALLOW;
	}

	public String name() {
		return this.name;
	}

	public MODE mode() {
		return mode == ALLOW ? MODE.ALLOW_ALL_BUT : MODE.DENY_ALL_BUT;
	}

	public OUser mode(MODE mode) {
		this.mode = mode == MODE.ALLOW_ALL_BUT ? ALLOW : DENY;
		return this;
	}

	public byte internalMode() {
		return mode;
	}

	public void internalMode(byte iMode) {
		mode = iMode;
	}

	public OUser inherit() {
		return inherit;
	}

	public OUser inherit(OUser inherit) {
		this.inherit = inherit;
		return this;
	}

	public String password() {
		return password;
	}

	public OUser password(String iPassword) {
		this.password = OSecurityManager.instance().digest2String(iPassword);
		return this;
	}

	public void passwordEncoded(String iPassword) {
		this.password = iPassword;
	}
}
