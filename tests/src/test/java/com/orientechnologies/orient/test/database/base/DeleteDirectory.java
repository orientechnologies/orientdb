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

import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class DeleteDirectory {
	@Parameters(value = "path")
	public DeleteDirectory(String iPath) {
		final File f = new File(iPath);

		if (f.exists())
			deleteDirectory(f);
		else
			System.err.println("Directory: " + f.getAbsolutePath() + " not found");
	}

	private void deleteDirectory(File iDirectory) {
		if (iDirectory.isDirectory())
			for (File f : iDirectory.listFiles()) {
				if (f.isDirectory())
					deleteDirectory(f);
				else
					f.delete();
			}

	}
}
