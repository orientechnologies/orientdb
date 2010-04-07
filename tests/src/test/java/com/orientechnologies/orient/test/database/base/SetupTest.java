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
package com.orientechnologies.orient.test.database.base;

import java.io.File;

import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class SetupTest {
	private String						url;
	private boolean						reuseDatabase	= false;

	private static SetupTest	instance;

	@Parameters(value = { "url", "reuseDatabase" })
	public SetupTest(String iURL, @Optional boolean iReuseDatabase) {
		instance = this;

		url = iURL;
		reuseDatabase = iReuseDatabase;
	}

	public void init() {
		new File(url).delete();
	}

	public String getURL() {
		if (url != null)
			return url;

		return System.getProperty("url");
	}

	public boolean isReuseDatabase() {
		return reuseDatabase;
	}

	public static SetupTest instance() {
		if (instance == null)
			instance = new SetupTest(null, false);

		return instance;
	}
}
