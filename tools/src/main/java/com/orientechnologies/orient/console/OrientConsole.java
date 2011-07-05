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
package com.orientechnologies.orient.console;

import com.orientechnologies.common.console.OConsoleApplication;
import com.orientechnologies.orient.client.remote.OEngineRemote;
import com.orientechnologies.orient.core.Orient;

public abstract class OrientConsole extends OConsoleApplication {

	public OrientConsole(String[] args) {
		super(args);
	}

	@Override
	protected void onException(Throwable e) {
		Throwable current = e;
		while (current != null) {
			err.println("Error: " + (current.getMessage() != null ? current.getMessage() : current.toString()));
			current = current.getCause();
		}
	}

	@Override
	protected void onBefore() {
		Orient.instance().registerEngine(new OEngineRemote());
		printApplicationInfo();
	}

	protected void printApplicationInfo() {
	}

	@Override
	protected void onAfter() {
		out.println();
	}

	@Override
	public void help() {
		super.help();
	}
}
