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
package com.orientechnologies.orient.graph.console;

import com.orientechnologies.common.console.TTYConsoleReader;
import com.orientechnologies.common.console.annotation.ConsoleCommand;
import com.orientechnologies.common.console.annotation.ConsoleParameter;
import com.orientechnologies.orient.console.OConsoleDatabaseApp;
import com.orientechnologies.orient.graph.gremlin.OCommandGremlin;
import com.orientechnologies.orient.graph.gremlin.OGremlinHelper;
import com.tinkerpop.gremlin.jsr223.GremlinScriptEngineFactory;

/**
 * Gremlin specialized console.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OGremlinConsole extends OConsoleDatabaseApp {

	public OGremlinConsole(final String[] args) {
		super(args);
	}

	public static void main(final String[] args) {
		try {
			boolean tty = false;
			try {
				if (setTerminalToCBreak())
					tty = true;

			} catch (Exception e) {
			}

			final OConsoleDatabaseApp console = new OGremlinConsole(args);
			if (tty)
				console.setReader(new TTYConsoleReader());

			console.run();

		} finally {
			try {
				stty("echo");
			} catch (Exception e) {
			}
		}
	}

	@Override
	protected void onBefore() {
		super.onBefore();

		out.println("\nInstalling extensions for GREMLIN language v." + new GremlinScriptEngineFactory().getEngineVersion());

		OGremlinHelper.global().create();
	}

	@ConsoleCommand(splitInWords = false, description = "Execute a GREMLIN script")
	public void gremlin(@ConsoleParameter(name = "script-text", description = "The script text to execute") final String iScriptText) {
		checkCurrentDatabase();

		if (iScriptText == null || iScriptText.length() == 0)
			return;

		long start = System.currentTimeMillis();

		currentResultSet.clear();

		Object result = currentDatabase.command(new OCommandGremlin(iScriptText)).execute();

		out.println("\n" + result);

		out.printf("\nScript executed in %f sec(s).", (float) (System.currentTimeMillis() - start) / 1000);
	}
}
