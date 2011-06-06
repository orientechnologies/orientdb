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
package com.orientechnologies.orient.server.network.protocol.http;

import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.enterprise.channel.text.OChannelTextServer;
import com.orientechnologies.orient.server.network.protocol.ONetworkProtocolData;
import com.orientechnologies.orient.server.network.protocol.http.multipart.OHttpMultipartBaseInputStream;

/**
 * Mantain information about current HTTP request.
 * 
 * @author Luca Garulli
 * 
 */
public class OHttpRequest {
	public OContextConfiguration					configuration;
	public OChannelTextServer							channel;
	public String													method;
	public String													authorization;
	public String													sessionId;
	public String													url;
	public String													httpVersion;
	public String													content;
	public OHttpMultipartBaseInputStream	multipartStream;
	public String													boundary;
	public String													databaseName;
	public boolean												isMultipart;
	public String													ifMatch;

	public ONetworkProtocolData						data;
	public ONetworkProtocolHttpAbstract		executor;

	public OHttpRequest(final ONetworkProtocolHttpAbstract iExecutor, final OChannelTextServer iChannel,
			final ONetworkProtocolData iData, final OContextConfiguration iConfiguration) {
		executor = iExecutor;
		channel = iChannel;
		data = iData;
		configuration = iConfiguration;
	}
}
