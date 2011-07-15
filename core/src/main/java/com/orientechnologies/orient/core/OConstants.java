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
package com.orientechnologies.orient.core;

public class OConstants {
	public static final int			SIZE_BYTE				= 1;
	public static final int			SIZE_CHAR				= 2;
	public static final int			SIZE_SHORT			= 2;
	public static final int			SIZE_INT				= 4;
	public static final int			SIZE_LONG				= 8;

	public static final String	ORIENT_VERSION	= "1.0rc4-SNAPSHOT";
	public static final String	ORIENT_URL			= "www.orientechnologies.com";

	public static String getVersion() {
		final StringBuilder buffer = new StringBuilder();
		buffer.append(OConstants.ORIENT_VERSION);

		final String buildNumber = System.getProperty("orientdb.build.number");

		if (buildNumber != null) {
			buffer.append(" (build ");
			buffer.append(buildNumber);
			buffer.append(")");
		}

		return buffer.toString();
	}
}
