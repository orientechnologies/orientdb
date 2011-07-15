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
package com.orientechnologies.orient.server.network.protocol.http.command.all;

import com.orientechnologies.orient.server.config.OServerCommandConfiguration;
import com.orientechnologies.orient.server.config.OServerEntryConfiguration;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAbstract;

/**
 * Forward the execution to another command.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OServerCommandForward extends OServerCommandAbstract {
	private final String[]	pattern;
	private final String		prefix;
	private String					forwardTo;

	public OServerCommandForward(final OServerCommandConfiguration iConfiguration) {
		pattern = new String[] { iConfiguration.pattern };
		prefix = iConfiguration.pattern.substring(iConfiguration.pattern.indexOf("|") + 1);

		// LOAD HTTP CACHE CONFIGURATION
		for (OServerEntryConfiguration par : iConfiguration.parameters) {
			if (par.name.equals("to"))
				forwardTo = par.value;
		}
	}

	@Override
	public boolean execute(final OHttpRequest iRequest) throws Exception {
		final StringBuilder forwardURL = new StringBuilder("/");

		forwardURL.append(forwardTo);

		if (prefix.endsWith("*")) {
			final int prefixLength = prefix.length() - 1;
			final int postfix = iRequest.url.indexOf(prefix.substring(0, prefixLength));
			if (postfix > -1)
				forwardURL.append(iRequest.url.substring(postfix + prefixLength));
		}

		iRequest.url = forwardURL.toString();
		return true;
	}

	@Override
	public String[] getNames() {
		return pattern;
	}
}
