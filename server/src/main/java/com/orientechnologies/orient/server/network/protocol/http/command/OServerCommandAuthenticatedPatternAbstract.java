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
package com.orientechnologies.orient.server.network.protocol.http.command;

import com.orientechnologies.orient.server.config.OServerCommandConfiguration;

public abstract class OServerCommandAuthenticatedPatternAbstract extends OServerCommandAuthenticatedDbAbstract {

	private String[]	pattern;

	public OServerCommandAuthenticatedPatternAbstract(final OServerCommandConfiguration iConfig) {
		if (iConfig.pattern == null)
			throw new IllegalArgumentException("Command pattern missed");

		pattern = iConfig.pattern.split(" ");
	}

	public OServerCommandAuthenticatedPatternAbstract(String[] pattern) {
		this.pattern = pattern;
	}

	@Override
	public String[] getNames() {
		return pattern;
	}
}
