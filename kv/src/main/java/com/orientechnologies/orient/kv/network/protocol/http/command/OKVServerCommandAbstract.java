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
package com.orientechnologies.orient.kv.network.protocol.http.command;

import com.orientechnologies.orient.kv.network.protocol.http.OKVDictionary;
import com.orientechnologies.orient.server.network.protocol.ONetworkProtocolException;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAbstract;

public abstract class OKVServerCommandAbstract extends OServerCommandAbstract {
	protected OKVDictionary	dictionary;

	protected OKVServerCommandAbstract(final OKVDictionary dictionary) {
		this.dictionary = dictionary;
	}

	public static String[] getDbBucketKey(String iParameters, final int iMin) {
		if (iParameters == null || iParameters.length() < 11)
			throw new ONetworkProtocolException("Requested URI '" + iParameters + "' is invalid. Expected: entry/db/bucket/key");

		// REMOVE THE FIRST /
		if (iParameters.startsWith(OHttpUtils.URL_SEPARATOR))
			iParameters = iParameters.substring(1);

		if (iParameters.startsWith("entry"))
			iParameters = iParameters.substring("entry".length()+1);

		if (iParameters.endsWith(OHttpUtils.URL_SEPARATOR))
			iParameters = iParameters.substring(0, iParameters.length() - 1);

		final String[] pars = iParameters.split(OHttpUtils.URL_SEPARATOR);
		
		if (pars == null || pars.length < iMin)
			throw new ONetworkProtocolException("Requested URI '" + iParameters + "' is invalid. Expected: entry/db/bucket[/key]");

		return pars;
	}
}
