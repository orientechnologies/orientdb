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
import java.util.Map.Entry;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.server.config.OServerCommandConfiguration;
import com.orientechnologies.orient.server.config.OServerEntryConfiguration;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;

public class OServerCommandGetStaticContent extends OServerCommandConfigurableAbstract {
	private static final String[]										DEF_PATTERN				= { "GET|www", "GET|studio/", "GET|", "GET|*.htm",
			"GET|*.html", "GET|*.xml", "GET|*.jpeg", "GET|*.jpg", "GET|*.png", "GET|*.gif", "GET|*.js", "GET|*.otf", "GET|*.css",
			"GET|*.swf", "GET|favicon.ico", "GET|robots.txt"							};

	private static final String											CONFIG_HTTP_CACHE	= "http.cache:";
	private static final String											CONFIG_ROOT_PATH	= "root.path";
	private static final String											CONFIG_FILE_PATH	= "file.path";

	private Map<String, OStaticContentCachedEntry>	cacheContents;
	private Map<String, String>											cacheHttp					= new HashMap<String, String>();
	private String																	cacheHttpDefault	= "Cache-Control: max-age=3000";
	private String																	rootPath;
	private String																	filePath;

	public OServerCommandGetStaticContent() {
		super(DEF_PATTERN);
	}

	public OServerCommandGetStaticContent(final OServerCommandConfiguration iConfiguration) {
		super(iConfiguration.pattern);

		// LOAD HTTP CACHE CONFIGURATION
		for (OServerEntryConfiguration par : iConfiguration.parameters) {
			if (par.name.startsWith(CONFIG_HTTP_CACHE)) {
				final String filter = par.name.substring(CONFIG_HTTP_CACHE.length());
				if (filter.equalsIgnoreCase("default"))
					cacheHttpDefault = par.value;
				else if (filter.length() > 0) {
					final String[] filters = filter.split(" ");
					for (String f : filters) {
						cacheHttp.put(f, par.value);
					}
				}
			} else if (par.name.startsWith(CONFIG_ROOT_PATH))
				rootPath = par.value;
			else if (par.name.startsWith(CONFIG_FILE_PATH))
				filePath = par.value;

		}
	}

	@Override
	public boolean execute(final OHttpRequest iRequest) throws Exception {
		iRequest.data.commandInfo = "Get static content";
		iRequest.data.commandDetail = iRequest.url;

		if (filePath == null && rootPath == null) {
			// GET GLOBAL CONFIG
			rootPath = iRequest.configuration.getValueAsString("orientdb.www.path", "src/site");
			if (rootPath == null) {
				OLogManager.instance().warn(this,
						"No path configured. Specify the 'root.path', 'file.path' or the global 'orientdb.www.path' variable", rootPath);
				return false;
			}
		}

		if (filePath == null) {
			// CHECK DIRECTORY
			final File wwwPathDirectory = new File(rootPath);
			if (!wwwPathDirectory.exists())
				OLogManager.instance().warn(this, "path variable points to '%s' but it doesn't exists", rootPath);
			if (!wwwPathDirectory.isDirectory())
				OLogManager.instance().warn(this, "path variable points to '%s' but it isn't a directory", rootPath);
		}

		if (cacheContents == null && OGlobalConfiguration.SERVER_CACHE_FILE_STATIC.getValueAsBoolean())
			// CREATE THE CACHE IF ENABLED
			cacheContents = new HashMap<String, OStaticContentCachedEntry>();

		InputStream is = null;
		long contentSize = 0;
		String type = null;

		try {
			String path;
			if (filePath != null)
				// SINGLE FILE
				path = filePath;
			else {
				// GET FROM A DIRECTORY
				final String url = getResource(iRequest);
				if (url.startsWith("/www"))
					path = rootPath + url.substring("/www".length(), url.length());
				else
					path = rootPath + url;
			}

			if (cacheContents != null) {
				synchronized (cacheContents) {
					final OStaticContentCachedEntry cachedEntry = cacheContents.get(path);
					if (cachedEntry != null) {
						is = new ByteArrayInputStream(cachedEntry.content);
						contentSize = cachedEntry.size;
						type = cachedEntry.type;
					}
				}
			}

			if (is == null) {
				File inputFile = new File(path);
				if (!inputFile.exists()) {
					OLogManager.instance().debug(this, "Static resource not found: %s", path);

					sendBinaryContent(iRequest, 404, "File not found", null, null, 0);
					return false;
				}

				if (filePath == null && inputFile.isDirectory()) {
					inputFile = new File(path + "/index.htm");
					if (inputFile.exists())
						path = path + "/index.htm";
					else {
						inputFile = new File(path + "/index.html");
						if (inputFile.exists())
							path = path + "/index.html";
					}
				}

				if (path.endsWith(".htm") || path.endsWith(".html"))
					type = "text/html";
				else if (path.endsWith(".png"))
					type = "image/png";
				else if (path.endsWith(".jpeg"))
					type = "image/jpeg";
				else if (path.endsWith(".js"))
					type = "application/x-javascript";
				else if (path.endsWith(".css"))
					type = "text/css";
				else if (path.endsWith(".ico"))
					type = "image/x-icon";
				else if (path.endsWith(".otf"))
					type = "font/opentype";
				else
					type = "text/plain";

				is = new BufferedInputStream(new FileInputStream(inputFile));
				contentSize = inputFile.length();

				if (cacheContents != null) {
					// READ THE ENTIRE STREAM AND CACHE IT IN MEMORY
					final byte[] buffer = new byte[(int) contentSize];
					for (int i = 0; i < contentSize; ++i)
						buffer[i] = (byte) is.read();

					OStaticContentCachedEntry cachedEntry = new OStaticContentCachedEntry();
					cachedEntry.content = buffer;
					cachedEntry.size = contentSize;
					cachedEntry.type = type;

					cacheContents.put(path, cachedEntry);

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
		return false;
	}

	@Override
	protected void onBeforeResponseHeader(OHttpRequest iRequest) throws IOException {
		String header = cacheHttpDefault;

		if (cacheHttp.size() > 0) {
			final String resource = getResource(iRequest);

			// SEARCH IN CACHE IF ANY
			for (Entry<String, String> entry : cacheHttp.entrySet()) {
				final int wildcardPos = entry.getKey().indexOf('*');
				final String partLeft = entry.getKey().substring(0, wildcardPos);
				final String partRight = entry.getKey().substring(wildcardPos + 1);

				if (resource.startsWith(partLeft) && resource.endsWith(partRight)) {
					// FOUND
					header = entry.getValue();
					break;
				}
			}
		}

		writeLine(iRequest, header);
	}

	protected String getResource(final OHttpRequest iRequest) {
		final String url;
		if (OHttpUtils.URL_SEPARATOR.equals(iRequest.url))
			url = "/www/index.htm";
		else {
			int pos = iRequest.url.indexOf('?');
			if (pos > -1)
				url = iRequest.url.substring(0, pos);
			else
				url = iRequest.url;
		}
		return url;
	}

}
