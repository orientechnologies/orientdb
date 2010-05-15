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
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAbstract;

public class OServerCommandGetStaticContent extends OServerCommandAbstract {
	private static final String[]	NAMES			= { "GET.www", "GET.", "GET.favicon.ico" };

	static final String						WWW_PATH	= System.getProperty("orient.www.path", "src/site");

	public void execute(final OHttpRequest iRequest) throws Exception {
		iRequest.data.commandInfo = "Get static content";
		iRequest.data.commandDetail = iRequest.url;

		InputStream bufferedFile = null;
		try {
			String url = OHttpUtils.URL_SEPARATOR.equals(iRequest.url) ? url = "/www/index.htm" : iRequest.url;
			url = WWW_PATH + url.substring("www".length() + 1, url.length());
			final File inputFile = new File(url);
			if (!inputFile.exists()) {
				sendStatus(iRequest, 404, "File not found");
				iRequest.channel.flush();
				return;
			}

			String type = null;
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

			bufferedFile = new BufferedInputStream(new FileInputStream(inputFile));

			sendBinaryContent(iRequest, OHttpUtils.STATUS_OK_CODE, OHttpUtils.STATUS_OK_DESCRIPTION, type, bufferedFile, inputFile
					.length());

		} catch (IOException e) {
			e.printStackTrace();

		} finally {
			if (bufferedFile != null)
				try {
					bufferedFile.close();
				} catch (IOException e) {
				}
		}
	}

	public String[] getNames() {
		return NAMES;
	}
}
