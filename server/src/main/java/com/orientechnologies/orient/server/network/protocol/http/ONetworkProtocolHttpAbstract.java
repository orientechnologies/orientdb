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

import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URLDecoder;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.enterprise.channel.OChannel;
import com.orientechnologies.orient.enterprise.channel.text.OChannelTextServer;
import com.orientechnologies.orient.server.OClientConnection;
import com.orientechnologies.orient.server.OClientConnectionManager;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerConfiguration;
import com.orientechnologies.orient.server.network.protocol.ONetworkProtocol;
import com.orientechnologies.orient.server.network.protocol.ONetworkProtocolException;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommand;

public abstract class ONetworkProtocolHttpAbstract extends ONetworkProtocol {
	private static final int				MAX_CONTENT_LENGTH	= 10000;																	// MAX = 10Kb
	private static final int				TCP_DEFAULT_TIMEOUT	= 5000;

	protected OClientConnection			connection;
	protected OServerConfiguration	configuration;
	protected OChannelTextServer		channel;
	protected OUser									account;

	protected OHttpRequest					request;

	Map<String, OServerCommand>			commands						= new HashMap<String, OServerCommand>();

	public ONetworkProtocolHttpAbstract() {
		super(OServer.getThreadGroup(), "HTTP");
	}

	public void config(final Socket iSocket, final OClientConnection iConnection) throws IOException {
		iSocket.setSoTimeout(TCP_DEFAULT_TIMEOUT);
		channel = (OChannelTextServer) new OChannelTextServer(iSocket);
		connection = iConnection;
		configuration = new OServerConfiguration();

		request = new OHttpRequest(channel, data);

		start();
	}

	public void service() throws ONetworkProtocolException, IOException {
		OProfiler.getInstance().updateStatistic("Server.requests", +1);

		++data.totalRequests;
		data.commandType = -1;
		data.lastCommandType = -1;
		data.lastCommandDetail = request.url;
		data.lastCommandExecutionTime = 0;

		long begin = System.currentTimeMillis();

		final String command;
		if (request.url.length() < 2) {
			command = "";
		} else {
			final int sep = request.url.indexOf("/", 1);
			command = sep == -1 ? request.url.substring(1) : request.url.substring(1, sep);
		}

		OServerCommand cmd = commands.get(request.method + "." + command);

		if (cmd != null)
			try {
				// EXECUTE THE COMMAND
				cmd.execute(request);

			} catch (Exception e) {

				final StringBuilder buffer = new StringBuilder();
				buffer.append(e);
				Throwable cause = e.getCause();
				while (cause != null && cause != cause.getCause()) {
					buffer.append("\r\n--> ");
					buffer.append(cause);

					cause = cause.getCause();
				}

				sendTextContent(500, "Unknow error", OHttpUtils.CONTENT_TEXT_PLAIN, buffer.toString());
			}
		else
			OLogManager.instance()
					.warn(this, "->" + channel.socket.getInetAddress().getHostAddress() + ": Command not found: " + command);

		data.lastCommandType = data.commandType;
		data.lastCommandExecutionTime = System.currentTimeMillis() - begin;
		data.totalCommandExecutionTime += data.lastCommandExecutionTime;
	}

	/**
	 * Register all the names for the same instance
	 * 
	 * @param iServerCommandInstance
	 */
	protected void registerCommand(final OServerCommand iServerCommandInstance) {
		for (String name : iServerCommandInstance.getNames())
			commands.put(name, iServerCommandInstance);
	}

	protected void sendTextContent(final int iCode, final String iReason, final String iContentType, final String iContent)
			throws IOException {
		sendStatus(iCode, iReason);
		sendResponseHeaders(iContentType);
		writeLine(OHttpUtils.CONTENT_LENGTH + (iContent != null ? iContent.length() + 1 : 0));
		writeLine(null);

		if (iContent != null && iContent.length() > 0) {
			writeLine(iContent);
		}

		channel.flush();
	}

	protected void writeLine(final String iContent) throws IOException {
		if (iContent != null)
			channel.outStream.write(iContent.getBytes());
		channel.outStream.write(OHttpUtils.EOL);
	}

	protected void sendStatus(final int iStatus, final String iReason) throws IOException {
		writeLine(request.httpVersion + " " + iStatus + " " + iReason);
	}

	protected void sendResponseHeaders(final String iContentType) throws IOException {
		writeLine("Cache-Control: no-cache, no-store, max-age=0, must-revalidate");
		writeLine("Pragma: no-cache");
		writeLine("Date: " + new Date());
		writeLine("Content-Type: " + iContentType);
		writeLine("Server: " + data.serverInfo);
		writeLine("Connection: Keep-Alive");
	}

	protected String readAllContent() throws IOException {
		StringBuilder request = new StringBuilder();
		int in;
		char currChar;
		int contentLength = -1;
		boolean endOfHeaders = false;
		while (!channel.socket.isInputShutdown()) {
			in = channel.inStream.read();
			if (in == -1)
				break;

			currChar = (char) in;

			if (currChar == '\r') {
				if (request.length() > 0 && contentLength == -1) {
					String line = request.toString().toUpperCase();
					if (line.startsWith(OHttpUtils.CONTENT_LENGTH)) {
						contentLength = Integer.parseInt(line.substring(OHttpUtils.CONTENT_LENGTH.length()));
						if (contentLength > MAX_CONTENT_LENGTH)
							OLogManager.instance().warn(
									this,
									"->" + channel.socket.getInetAddress().getHostAddress() + ": Error on content size " + contentLength
											+ ": the maximum allowed is " + MAX_CONTENT_LENGTH);
					}
				}

				// CONSUME /r or /n
				in = channel.inStream.read();
				if (in == -1)
					break;

				currChar = (char) in;

				if (!endOfHeaders && request.length() == 0) {
					if (contentLength <= 0)
						return null;

					// FIRST BLANK LINE: END OF HEADERS
					endOfHeaders = true;
				}

				request.setLength(0);
			} else if (endOfHeaders && request.length() == 0 && currChar != '\r' && currChar != '\n') {
				// END OF HEADERS
				byte[] buffer = new byte[contentLength];
				buffer[0] = (byte) currChar;
				channel.inStream.read(buffer, 1, contentLength - 1);
				return new String(buffer);
			} else
				request.append(currChar);
		}

		if (OLogManager.instance().isDebugEnabled())
			OLogManager.instance().debug(this,
					"Error on parsing HTTP content from client " + channel.socket.getInetAddress().getHostAddress() + ":\n" + request);

		return null;
	}

	@Override
	protected void execute() throws Exception {
		if (channel.socket.isInputShutdown()) {
			connectionClosed();
			return;
		}

		long timer = -1;

		try {
			StringBuilder requestContent = new StringBuilder();
			char c = (char) channel.inStream.read();

			if (channel.inStream.available() == 0) {
				// connectionClosed();
				return;
			}

			timer = OProfiler.getInstance().startChrono();

			requestContent.setLength(0);

			if (c != '\n')
				// AVOID INITIAL /N
				requestContent.append(c);

			while (!channel.socket.isInputShutdown()) {
				c = (char) channel.inStream.read();

				if (c == '\r') {
					String[] words = requestContent.toString().split(" ");
					if (words.length < 3) {
						OLogManager.instance().warn(this,
								"->" + channel.socket.getInetAddress().getHostAddress() + ": Error on invalid content:\n" + requestContent);
						break;
					}

					// CONSUME THE NEXT \n
					channel.inStream.read();

					request.method = words[0];
					request.url = URLDecoder.decode(words[1], "UTF-8").trim();
					request.httpVersion = words[2];
					request.content = readAllContent();
					if (request.content != null)
						request.content = URLDecoder.decode(request.content, "UTF-8").trim();

					service();
					return;
				}
				requestContent.append(c);
			}

			if (OLogManager.instance().isDebugEnabled())
				OLogManager.instance().debug(this,
						"Parsing request from client " + channel.socket.getInetAddress().getHostAddress() + ":\n" + requestContent);

		} catch (SocketException e) {
			connectionError();

		} catch (SocketTimeoutException e) {
			timeout();

		} catch (Throwable t) {
			if (request.method != null && request.url != null)
				try {
					sendTextContent(505, "Error on excuting of " + request.method + " for the resource: " + request.url, "text/plain", t
							.toString());
				} catch (IOException e) {
				}
		} finally {
			if (timer > -1)
				OProfiler.getInstance().stopChrono("ONetworkProtocolHttp.execute", timer);
		}
	}

	protected void connectionClosed() {
		OProfiler.getInstance().updateStatistic("OrientKV-Server.http.closed", +1);
		sendShutdown();
	}

	protected void timeout() {
		OProfiler.getInstance().updateStatistic("OrientKV-Server.http.timeout", +1);
		sendShutdown();
	}

	protected void connectionError() {
		OProfiler.getInstance().updateStatistic("OrientKV-Server.http.error", +1);
		sendShutdown();
	}

	@Override
	public void sendShutdown() {
		super.sendShutdown();

		try {
			channel.socket.close();
		} catch (IOException e) {
		}

		if (OLogManager.instance().isDebugEnabled())
			OLogManager.instance().debug(this, "Connection shutdowned");
	}

	@Override
	public void shutdown() {
		sendShutdown();
		channel.close();

		OClientConnectionManager.instance().onClientDisconnection(connection.id);
	}

	@Override
	public OChannel getChannel() {
		return null;
	}
}
