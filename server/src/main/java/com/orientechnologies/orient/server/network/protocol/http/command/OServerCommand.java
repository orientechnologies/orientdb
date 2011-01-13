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

import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;

/**
 * Generic interface for server-side commands.
 * 
 * @author Luca Garulli
 * 
 */
public interface OServerCommand {
	/**
	 * Called before to execute. Useful to make checks.
	 */
	public boolean beforeExecute(OHttpRequest iRequest) throws Exception;

	/**
	 * Executes the command requested.
	 * 
	 * @return boolean value that indicates if this command is part of a chain
	 */
	public boolean execute(OHttpRequest iRequest) throws Exception;

	public String[] getNames();
}
