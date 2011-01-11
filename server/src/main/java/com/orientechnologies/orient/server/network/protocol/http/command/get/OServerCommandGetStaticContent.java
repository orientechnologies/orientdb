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
package com.orientechnologies.orient.server.network.protocol.http.command.get;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAbstract;

public class OServerCommandGetStaticContent extends OServerCommandAbstract {
	private static final String[]										NAMES			= { "GET|www", "GET|", "GET|*.htm", "GET|*.xml", "GET|*.jpeg",
			"GET|*.jpg", "GET|*.png", "GET|*.gif", "GET|*.js", "GET|*.css", "GET|favicon.ico", "GET|robots.txt" };

	static final String															WWW_PATH	= System.getProperty("orientdb.www.path", "src/site");

	private Map<String, OStaticContentCachedEntry>	cache;

	public OServerCommandGetStaticContent() {
		useCache = true;
	}

	@Override
	public void execute(final OHttpRequest iRequest) throws Exception {
		iRequest.data.commandInfo = "Get static content";
		iRequest.data.commandDetail = iRequest.url;

		if (cache == null && OGlobalConfiguration.SERVER_CACHE_STATIC_RESOURCES.getValueAsBoolean())
			// CREATE THE CACHE IF ENABLED
			cache = new HashMap<String, OStaticContentCachedEntry>();

		InputStream is = null;
		long contentSize = 0;
		String type = null;

		try {
			String url = OHttpUtils.URL_SEPARATOR.equals(iRequest.url) ? url = "/www/index.htm" : iRequest.url;
			if (url.contains("www"))
				url = WWW_PATH + url.substring("www".length() + 1, url.length());
			else
				url = WWW_PATH + url;

			if (cache != null) {
				synchronized (cache) {
					final OStaticContentCachedEntry cachedEntry = cache.get(url);
					if (cachedEntry != null) {
						is = new ByteArrayInputStream(cachedEntry.content);
						contentSize = cachedEntry.size;
						type = cachedEntry.type;
					}
				}
			}

			if (is == null) {
				final File inputFile = new File(url);
				if (!inputFile.exists()) {
					OLogManager.instance().debug(this, "Static resource not found: %s", url);

					sendBinaryContent(iRequest, 404, "File not found", null, null, 0);
					return;
				}

				if (url.endsWith(".htm") || url.endsWith(".html"))
					type = "text/html";
				else if (url.endsWith(".png"))
					type = "image/png";
				else if (url.endsWith(".jpeg"))
					type = "image/jpeg";
				else if (url.endsWith(".js"))
					type = "application/x-javascript";
				else if (url.endsWith(".css"))
					type = "text/css";

				is = new BufferedInputStream(new FileInputStream(inputFile));
				contentSize = inputFile.length();

				if (cache != null) {
					// READ AL THE STREAM AND CACHE IT IN MEMORY
					byte[] buffer = new byte[(int) contentSize];
					for (int i = 0; i < contentSize; ++i)
						buffer[i] = (byte) is.read();

					OStaticContentCachedEntry cachedEntry = new OStaticContentCachedEntry();
					cachedEntry.content = buffer;
					cachedEntry.size = contentSize;
					cachedEntry.type = type;

					cache.put(url, cachedEntry);

					is = new ByteArrayInputStream(cachedEntry.content);
				}
			}

			sendBinaryContent(iRequest, OHttpUtils.STATUS_OK_CODE, OHttpUtils.STATUS_OK_DESCRIPTION, type, is, contentSize);

		} catch (IOException e) {
			e.printStackTrace();

		} finally {
			if (is != null)
				try {
					is.close();
				} catch (IOException e) {
				}
		}
	}

	@Override
	public String[] getNames() {
		return NAMES;
	}

	/**
	 * Public access, avoid checks.
	 */
	@Override
	public boolean beforeExecute(OHttpRequest iRequest) throws IOException {
		return true;
	}
}
