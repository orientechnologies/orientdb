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

import com.orientechnologies.orient.core.hook.ODocumentHookAbstract;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.security.OSecurityManager;

/**
 * Encrypt the password using the SHA-256 algorithm.
 * 
 * @author Luca Garulli
 */
public class OUserTrigger extends ODocumentHookAbstract {

	@Override
	public boolean onRecordBeforeCreate(ODocument iDocument) {
		return encodePassword(iDocument);
	}

	@Override
	public boolean onRecordBeforeUpdate(final ODocument iDocument) {
		return encodePassword(iDocument);
	}

	private boolean encodePassword(final ODocument iDocument) {
		if ("OUser".equals(iDocument.getClassName())) {
			final String password = (String) iDocument.field("password");
			if (!password.startsWith(OSecurityManager.ALGORITHM_PREFIX)) {
				iDocument.field("password", OUser.encryptPassword(password));
				return true;
			}
		}
		return false;
	}
}
