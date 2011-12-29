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

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.orientechnologies.orient.core.annotation.OBeforeDeserialization;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.type.ODocumentWrapper;

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
@SuppressWarnings("unchecked")
public class ORole extends ODocumentWrapper {
	public static final String	ADMIN	= "admin";

	public enum ALLOW_MODES {
		DENY_ALL_BUT, ALLOW_ALL_BUT
	}

	// CRUD OPERATIONS
	public final static int							PERMISSION_NONE		= 0;
	public final static int							PERMISSION_CREATE	= registerPermissionBit(0, "Create");
	public final static int							PERMISSION_READ		= registerPermissionBit(1, "Read");
	public final static int							PERMISSION_UPDATE	= registerPermissionBit(2, "Update");
	public final static int							PERMISSION_DELETE	= registerPermissionBit(3, "Delete");
	public final static int							PERMISSION_ALL		= PERMISSION_CREATE + PERMISSION_READ + PERMISSION_UPDATE
																														+ PERMISSION_DELETE;

	protected final static byte					STREAM_DENY				= 0;
	protected final static byte					STREAM_ALLOW			= 1;

	private static Map<Integer, String>	PERMISSION_BIT_NAMES;

	protected ALLOW_MODES								mode							= ALLOW_MODES.DENY_ALL_BUT;
	protected ORole											parentRole;
	protected Map<String, Byte>					rules							= new LinkedHashMap<String, Byte>();

	/**
	 * Constructor used in unmarshalling.
	 */
	public ORole() {
	}

	public ORole(final String iName, final ORole iParent, final ALLOW_MODES iAllowMode) {
		super("ORole");
		document.field("name", iName);
		parentRole = iParent;
		document.field("inheritedRole", parentRole != null ? parentRole.getName() : null);
		setMode(iAllowMode);
		document.field("rules", new HashMap<String, Number>());
	}

	/**
	 * Create the role by reading the source document.
	 */
	public ORole(final ODocument iSource) {
		fromStream(iSource);
	}

	@Override
	@OBeforeDeserialization
	public void fromStream(final ODocument iSource) {
		if (document != null)
			return;

		document = iSource;

		mode = ((Number) document.field("mode")).byteValue() == STREAM_ALLOW ? ALLOW_MODES.ALLOW_ALL_BUT : ALLOW_MODES.DENY_ALL_BUT;

		final String roleName = document.field("inheritedRole");
		parentRole = roleName != null ? document.getDatabase().getMetadata().getSecurity().getRole(roleName) : null;

		final Map<String, Number> storedRules = document.field("rules");
		if (storedRules != null)
			for (Entry<String, Number> a : storedRules.entrySet()) {
				rules.put(a.getKey(), a.getValue().byteValue());
			}
	}

	public boolean allow(final String iResource, final int iCRUDOperation) {
		// CHECK FOR SECURITY AS DIRECT RESOURCE
		final Byte access = rules.get(iResource);
		if (access != null) {
			final byte mask = (byte) iCRUDOperation;

			return (access.byteValue() & mask) == mask;
		}

		return mode == ALLOW_MODES.ALLOW_ALL_BUT;
	}

	public boolean hasRule(final String iResource) {
		return rules.containsKey(iResource);
	}

	public void addRule(final String iResource, final int iOperation) {
		rules.put(iResource, (byte) iOperation);
		document.field("rules", rules);
	}

	/**
	 * Grant a permission to the resource.
	 * 
	 * @param iResource
	 *          Requested resource
	 * @param iOperation
	 *          Permission to grant/add
	 */
	public void grant(final String iResource, final int iOperation) {
		final Byte current = rules.get(iResource);
		byte currentValue = current == null ? PERMISSION_NONE : current.byteValue();

		currentValue |= (byte) iOperation;

		rules.put(iResource, currentValue);
		document.field("rules", rules);
	}

	/**
	 * Revoke a permission to the resource.
	 * 
	 * @param iResource
	 *          Requested resource
	 * @param iOperation
	 *          Permission to grant/remove
	 */
	public void revoke(final String iResource, final int iOperation) {
		if (iOperation == PERMISSION_NONE)
			return;

		final Byte current = rules.get(iResource);

		byte currentValue;
		if (current == null)
			currentValue = PERMISSION_NONE;
		else {
			currentValue = current.byteValue();
			currentValue &= ~(byte) iOperation;
		}

		rules.put(iResource, currentValue);
		document.field("rules", rules);
	}

	public String getName() {
		return document.field("name");
	}

	public ALLOW_MODES getMode() {
		return mode;
	}

	public ORole setMode(final ALLOW_MODES iMode) {
		this.mode = iMode;
		document.field("mode", mode == ALLOW_MODES.ALLOW_ALL_BUT ? STREAM_ALLOW : STREAM_DENY);
		return this;
	}

	public ORole getParentRole() {
		return parentRole;
	}

	public ORole setParentRole(final ORole iParent) {
		this.parentRole = iParent;
		document.field("inheritedRole", parentRole != null ? parentRole.getName() : null);
		return this;
	}

	@Override
	public ORole save() {
		document.save(ORole.class.getSimpleName());
		return this;
	}

	public Map<String, Byte> getRules() {
		return Collections.unmodifiableMap(rules);
	}

	@Override
	public String toString() {
		return getName();
	}

	/**
	 * Convert the permission code to a readable string.
	 * 
	 * @param iPermission
	 *          Permission to convert
	 * @return String representation of the permission
	 */
	public static String permissionToString(final int iPermission) {
		int permission = iPermission;
		final StringBuilder returnValue = new StringBuilder();
		for (Entry<Integer, String> p : PERMISSION_BIT_NAMES.entrySet()) {
			if ((permission & p.getKey()) == p.getKey()) {
				if (returnValue.length() > 0)
					returnValue.append(", ");
				returnValue.append(p.getValue());
				permission &= ~p.getKey();
			}
		}
		if (permission != 0) {
			if (returnValue.length() > 0)
				returnValue.append(", ");
			returnValue.append("Unknown 0x");
			returnValue.append(Integer.toHexString(permission));
		}

		return returnValue.toString();
	}

	public static int registerPermissionBit(final int iBitNo, final String iName) {
		if (iBitNo < 0 || iBitNo > 31)
			throw new IndexOutOfBoundsException("Permission bit number must be positive and less than 32");

		final int value = 1 << iBitNo;
		if (PERMISSION_BIT_NAMES == null)
			PERMISSION_BIT_NAMES = new HashMap<Integer, String>();

		if (PERMISSION_BIT_NAMES.containsKey(value))
			throw new IndexOutOfBoundsException("Permission bit number " + String.valueOf(iBitNo) + " already in use");

		PERMISSION_BIT_NAMES.put(value, iName);
		return value;
	}
}
