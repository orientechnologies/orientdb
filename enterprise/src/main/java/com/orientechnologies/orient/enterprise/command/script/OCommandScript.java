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
package com.orientechnologies.orient.enterprise.command.script;

import com.orientechnologies.orient.core.command.OCommandRequestTextAbstract;

/**
 * Script command request implementation. It just stores the request and delegated the execution to the configured OCommandExecutor.
 * 
 * 
 * @see OCommandExecutorScript
 * @author Luca Garulli
 * 
 */
@SuppressWarnings("serial")
public class OCommandScript extends OCommandRequestTextAbstract {
	private String	language;

	public OCommandScript() {
	}

	public OCommandScript(final String iLanguage, final String iText) {
		super(iText);
		language = iLanguage;
	}

	public OCommandScript(final String iText) {
		super(iText);
	}

	public String getLanguage() {
		return language;
	}

	public void setLanguage(String language) {
		this.language = language;
	}
}
