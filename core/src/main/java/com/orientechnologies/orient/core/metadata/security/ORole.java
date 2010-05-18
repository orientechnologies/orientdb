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
import java.util.Map.Entry;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Contains the user settings about security and permissions roles.<br/>
 * Allowed operation are the classic CRUD, namely:
 * <ul>
 * <li>CREATE</li>
 * <li>READ</li>
 * <li>UPDATE</li>
 * <li>DELETE</li>
 * </ul>
 * Mode = ALLOW (allow all but) or DENY (deny all but)
 */
public class ORole extends ODocument {
	public enum ALLOW_MODES {
		DENY_ALL_BUT, ALLOW_ALL_BUT
	}

	public enum CRUD_OPERATIONS {
		CREATE, READ, UPDATE, DELETE, ALL
	}

	// CRUD OPERATIONS
	public final static int			STREAM_CREATE	= 1;
	public final static int			STREAM_READ		= 2;
	public final static int			STREAM_UPDATE	= 4;
	public final static int			STREAM_DELETE	= 8;
	public final static int			STREAM_ALL		= STREAM_CREATE + STREAM_READ + STREAM_UPDATE + STREAM_DELETE;

	protected final static byte	STREAM_DENY		= 0;
	protected final static byte	STREAM_ALLOW	= 1;

	protected String						name;
	protected ALLOW_MODES				mode					= ALLOW_MODES.DENY_ALL_BUT;
	protected ORole							parentRole;
	protected Map<String, Byte>	acl						= new LinkedHashMap<String, Byte>();

	/**
	 * Constructor used in unmarshalling.
	 */
	public ORole(final ODatabaseRecord<?> iDatabase) {
		super(iDatabase, "ORole");
	}

	public ORole(final ODatabaseRecord<?> iDatabase, final String iName, final ORole iParent, final ALLOW_MODES iAllowMode) {
		this(iDatabase);
		name = iName;
		parentRole = iParent;
		mode = iAllowMode;
	}

	public boolean allow(final String iResource, final CRUD_OPERATIONS iCRUDOperation) {
		// CHECK FOR SECURITY AS DIRECT RESOURCE
		Byte access = acl.get(iResource);
		if (access != null) {
			byte mask = operation2Stream(iCRUDOperation);

			return (access.byteValue() & mask) == mask;
		}

		return mode == ALLOW_MODES.ALLOW_ALL_BUT;
	}

	public void addRule(final String iResource, final CRUD_OPERATIONS iOperation) {
		acl.put(iResource, operation2Stream(iOperation));
	}

	public String getName() {
		return this.name;
	}

	public ALLOW_MODES getMode() {
		return mode;
	}

	public ORole setMode(final ALLOW_MODES iMode) {
		this.mode = iMode;
		return this;
	}

	public ORole getParentRole() {
		return parentRole;
	}

	public ORole setParentRole(final ORole iParent) {
		this.parentRole = iParent;
		return this;
	}

	public ORole fromDocument(final ODocument iSource) {
		name = iSource.field("name");
		mode = ((Integer) iSource.field("mode")) == STREAM_ALLOW ? ALLOW_MODES.ALLOW_ALL_BUT : ALLOW_MODES.DENY_ALL_BUT;

		parentRole = database.getMetadata().getSecurity().getRole((String) iSource.field("inheritedRole"));

		Map<String, String> storedAcl = iSource.field("acl");
		for (Entry<String, String> a : storedAcl.entrySet()) {
			acl.put(a.getKey(), Byte.parseByte(a.getValue()));
		}
		return this;
	}

	public byte[] toStream() {
		field("name", name);
		field("mode", mode == ALLOW_MODES.ALLOW_ALL_BUT ? STREAM_ALLOW : STREAM_DENY);
		field("inheritedRole", parentRole != null ? parentRole.name : null);
		field("acl", acl);
		return super.toStream();
	}

	@Override
	public String toString() {
		return name;
	}

	private byte operation2Stream(final CRUD_OPERATIONS iOperation) {
		byte mask;
		switch (iOperation) {
		case CREATE:
			mask = STREAM_CREATE;
			break;
		case READ:
			mask = STREAM_READ;
			break;
		case UPDATE:
			mask = STREAM_UPDATE;
			break;
		case DELETE:
			mask = STREAM_DELETE;
			break;
		case ALL:
			mask = STREAM_ALL;
			break;
		default:
			mask = 0;
		}
		return mask;
	}
}
