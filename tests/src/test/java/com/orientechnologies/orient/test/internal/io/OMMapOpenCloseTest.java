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
package com.orientechnologies.orient.test.internal.io;

import java.io.File;
import java.io.IOException;

import org.testng.annotations.Test;

import com.orientechnologies.common.test.SpeedTestMonoThread;
import com.orientechnologies.orient.core.storage.fs.OFileMMap;

@Test(enabled = false)
public class OMMapOpenCloseTest extends SpeedTestMonoThread {
	protected static final String	FILE_NAME		= "C:/temp/orient-test.file";

	private static final int			NUMS				= 1000000;

	private static final int			START_SIZE	= 500000000;

	private OFileMMap							file;

	public OMMapOpenCloseTest() {
		super(NUMS);
	}

	@Override
	public void init() throws IOException {
		System.out.println("Testing opening and closing of a " + START_SIZE / 1000000 + "MB files for " + NUMS + " times...");

		file = new OFileMMap(FILE_NAME, "rw");

		// DELETE THE TEST FILE EVERY TIME
		File f = new File(FILE_NAME);
		if (!f.exists())
			file.create(START_SIZE);
		else
			file.open();
	}

	@Override
	public void cycle() throws IOException {
		// file.tryUnmap();
		// file.tryMap();
	}
}
