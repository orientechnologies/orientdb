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
package com.orientechnologies.orient.core.exception;

import com.orientechnologies.common.exception.OException;

public class OSecurityAccessException extends OException {

	private static final long	serialVersionUID	= -8486291378415776372L;
	private String						databaseName;

	public OSecurityAccessException(final String iDatabasename, final String message, final Throwable cause) {
		super(message, cause);
		databaseName = iDatabasename;
	}

	public OSecurityAccessException(final String iDatabasename, final String message) {
		super(message);
		databaseName = iDatabasename;
	}

	public OSecurityAccessException(final String message) {
		super(message);
	}

	public String getDatabaseName() {
		return databaseName;
	}
}
