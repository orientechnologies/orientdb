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
import java.util.Map.Entry;

import com.orientechnologies.common.concur.lock.OLockException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.exception.OSecurityAccessException;
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
	private static final String								COMMAND_SEPARATOR		= "|";
	private static final int									MAX_CONTENT_LENGTH	= 10000;																	// MAX = 10Kb
	private static final int									TCP_DEFAULT_TIMEOUT	= 10000;

	protected OClientConnection								connection;
	protected OServerConfiguration						configuration;
	protected OChannelTextServer							channel;
	protected OUser														account;
	protected OHttpRequest										request;

	private final StringBuilder								requestContent			= new StringBuilder();
	private final Map<String, OServerCommand>	exactCommands				= new HashMap<String, OServerCommand>();
	private final Map<String, OServerCommand>	wildcardCommands		= new HashMap<String, OServerCommand>();
	private final OBase64Utils								base64							= new OBase64Utils(null, "");

	public ONetworkProtocolHttpAbstract() {
		super(OServer.getThreadGroup(), "HTTP");
	}

	@Override
	public void config(final Socket iSocket, final OClientConnection iConnection) throws IOException {
		iSocket.setSoTimeout(TCP_DEFAULT_TIMEOUT);
		channel = new OChannelTextServer(iSocket);
		connection = iConnection;
		configuration = new OServerConfiguration();

		request = new OHttpRequest(this, channel, data);

		start();
	}

	public void service() throws ONetworkProtocolException, IOException {
		OProfiler.getInstance().updateStatistic("Server.requests", +1);

		++data.totalRequests;
		data.commandInfo = null;
		data.commandDetail = null;

		long begin = System.currentTimeMillis();

		final String command;
		if (request.url.length() < 2) {
			command = "";
		} else {
			command = request.url.substring(1);
		}

		final String commandString = request.method + COMMAND_SEPARATOR + command;

		// TRY TO FIND EXACT MATCH
		OServerCommand cmd = exactCommands.get(commandString);

		if (cmd == null) {
			// TRY WITH WILDCARD COMMANDS
			String partLeft, partRight;
			for (Entry<String, OServerCommand> entry : wildcardCommands.entrySet()) {
				int wildcardPos = entry.getKey().indexOf("*");
				partLeft = entry.getKey().substring(0, wildcardPos);
				partRight = entry.getKey().substring(wildcardPos + 1);

				if (commandString.startsWith(partLeft) && commandString.endsWith(partRight)) {
					cmd = entry.getValue();
					break;
				}
			}
		}

		if (cmd != null)
			try {
				if (cmd.beforeExecute(request)) {
					// EXECUTE THE COMMAND
					cmd.execute(request);
				}
			} catch (Exception e) {
				handleError(e);
			}
		else {
			try {
				OLogManager.instance().warn(this,
						"->" + channel.socket.getInetAddress().getHostAddress() + ": Command not found: " + request.method + "." + command);

				sendTextContent(405, "Command '" + command + "' not found", null, OHttpUtils.CONTENT_TEXT_PLAIN, "Command not found: "
						+ command);
			} catch (IOException e1) {
				sendShutdown();
			}
		}

		data.lastCommandInfo = data.commandInfo;
		data.lastCommandDetail = data.commandDetail;

		data.lastCommandExecutionTime = System.currentTimeMillis() - begin;
		data.totalCommandExecutionTime += data.lastCommandExecutionTime;
	}

	protected void handleError(Exception e) {
		if (OLogManager.instance().isDebugEnabled())
			OLogManager.instance().debug(this, "Caught exception", e);

		int errorCode = 500;
		String errorReason = null;
		String errorMessage = null;
		String responseHeaders = null;

		if (e instanceof ORecordNotFoundException)
			errorCode = 404;
		else if (e instanceof OLockException) {
			errorCode = 423;
			e = (Exception) e.getCause();
		} else if (e instanceof IllegalArgumentException)
			errorCode = 400;

		if (e instanceof ODatabaseException || e instanceof OSecurityAccessException || e instanceof OCommandExecutionException) {
			// GENERIC DATABASE EXCEPTION
			final Throwable cause = e instanceof OSecurityAccessException ? e : e.getCause();
			if (cause instanceof OSecurityAccessException) {
				// SECURITY EXCEPTION
				if (account == null) {
					// UNAUTHORIZED
					errorCode = OHttpUtils.STATUS_AUTH_CODE;
					errorReason = OHttpUtils.STATUS_AUTH_DESCRIPTION;
					responseHeaders = "WWW-Authenticate: Basic realm=\"OrientDB db-" + ((OSecurityAccessException) cause).getDatabaseName()
							+ "\"";
					errorMessage = null;
				} else {
					// USER ACCESS DENIED
					errorCode = 530;
					errorReason = "Current user has not the privileges to execute the request.";
					errorMessage = "530 User access denied";
				}
			}
		}

		if (errorReason == null)
			errorReason = "Unknow error";
		if (errorMessage == null) {
			// FORMAT GENERIC MESSAGE BY READING THE EXCEPTION STACK
			final StringBuilder buffer = new StringBuilder();
			buffer.append(e);
			Throwable cause = e.getCause();
			while (cause != null && cause != cause.getCause()) {
				buffer.append("\r\n--> ");
				buffer.append(cause);
				cause = cause.getCause();
			}
			errorMessage = buffer.toString();
		}

		try {
			sendTextContent(errorCode, errorReason, responseHeaders, OHttpUtils.CONTENT_TEXT_PLAIN, errorMessage);
		} catch (IOException e1) {
			sendShutdown();
		}
	}

	/**
	 * Register all the names for the same instance
	 * 
	 * @param iServerCommandInstance
	 */
	public void registerCommand(final Object iServerCommandInstance) {
		OServerCommand cmd = (OServerCommand) iServerCommandInstance;

		for (String name : cmd.getNames())
			if (name.contains("*"))
				wildcardCommands.put(name, cmd);
			else
				exactCommands.put(name, cmd);
	}

	protected void sendTextContent(final int iCode, final String iReason, String iHeaders, final String iContentType,
			final String iContent) throws IOException {
		final boolean empty = iContent == null || iContent.length() == 0;

		sendStatus(empty && iCode == 200 ? 204 : iCode, iReason);
		sendResponseHeaders(iContentType);
		if (iHeaders != null)
			writeLine(iHeaders);

		writeLine(OHttpUtils.CONTENT_LENGTH + (empty ? 0 : iContent.length()));

		writeLine(null);

		if (!empty)
			writeLine(iContent);

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

	protected void readAllContent(final OHttpRequest iRequest) throws IOException {
		iRequest.content = null;

		int in;
		char currChar;
		int contentLength = -1;
		boolean endOfHeaders = false;

		final StringBuilder request = new StringBuilder();

		while (!channel.socket.isInputShutdown()) {
			in = channel.inStream.read();
			if (in == -1)
				break;

			currChar = (char) in;

			if (currChar == '\r') {
				if (request.length() > 0 && !endOfHeaders) {
					String line = request.toString();
					String lineUpperCase = line.toUpperCase();
					if (lineUpperCase.startsWith("AUTHORIZATION")) {
						// STORE AUTHORIZATION INFORMATION INTO THE REQUEST
						String auth = line.substring("AUTHORIZATION".length() + 2);
						if (!auth.toUpperCase().startsWith("BASIC"))
							throw new IllegalArgumentException("Only HTTP Basic authorization is supported");

						iRequest.authorization = auth.substring("BASIC".length() + 1);

						iRequest.authorization = base64.decodeBase64("", iRequest.authorization);

					} else if (lineUpperCase.startsWith("COOKIE:")) {
						String sessionPair = line.substring("COOKIE:".length() + 1);
						String[] sessionPairItems = sessionPair.split("=");
						if (sessionPairItems.length == 2 && "OSESSIONID".equals(sessionPairItems[0]))
							iRequest.sessionId = sessionPairItems[1];

					} else if (lineUpperCase.startsWith(OHttpUtils.CONTENT_LENGTH)) {
						contentLength = Integer.parseInt(lineUpperCase.substring(OHttpUtils.CONTENT_LENGTH.length()));
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
						return;

					// FIRST BLANK LINE: END OF HEADERS
					endOfHeaders = true;
				}

				request.setLength(0);
			} else if (endOfHeaders && request.length() == 0 && currChar != '\r' && currChar != '\n') {
				// END OF HEADERS
				byte[] buffer = new byte[contentLength];
				buffer[0] = (byte) currChar;
				channel.inStream.read(buffer, 1, contentLength - 1);

				iRequest.content = new String(buffer);
				return;
			} else
				request.append(currChar);
		}

		if (OLogManager.instance().isDebugEnabled())
			OLogManager.instance().debug(this,
					"Error on parsing HTTP content from client " + channel.socket.getInetAddress().getHostAddress() + ":\n" + request);

		return;
	}

	@Override
	protected void execute() throws Exception {
		if (channel.socket.isInputShutdown()) {
			connectionClosed();
			return;
		}

		data.commandInfo = "Listening";
		data.commandDetail = null;

		try {
			data.lastCommandReceived = -1;

			char c = (char) channel.inStream.read();

			if (channel.inStream.available() == 0) {
				connectionClosed();
				return;
			}

			data.lastCommandReceived = OProfiler.getInstance().startChrono();

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
					readAllContent(request);
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
			if (request.method != null && request.url != null) {
				try {
					sendTextContent(505, "Error on executing of " + request.method + " for the resource: " + request.url, null, "text/plain",
							t.toString());
				} catch (IOException e) {
				}
			} else
				sendTextContent(505, "Error on executing request", null, "text/plain", t.toString());

			readAllContent(request);
		} finally {
			if (data.lastCommandReceived > -1)
				OProfiler.getInstance().stopChrono("ONetworkProtocolHttp.execute", data.lastCommandReceived);
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
			// FORCE SOCKET CLOSING
			channel.socket.close();
		} catch (final Exception e) {
		}
	}

	@Override
	public void shutdown() {
		try {
			sendShutdown();
			channel.close();

		} finally {
			OClientConnectionManager.instance().onClientDisconnection(connection.id);

			if (OLogManager.instance().isDebugEnabled())
				OLogManager.instance().debug(this, "Connection shutdowned");
		}
	}

	@Override
	public OChannel getChannel() {
		return channel;
	}

	public OUser getAccount() {
		return account;
	}
}
