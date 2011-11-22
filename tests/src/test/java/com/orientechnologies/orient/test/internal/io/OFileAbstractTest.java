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

import org.testng.Assert;
import org.testng.annotations.Test;

import com.orientechnologies.common.test.SpeedTestMonoThread;
import com.orientechnologies.orient.core.serialization.OBinaryProtocol;
import com.orientechnologies.orient.core.storage.fs.OFile;

@Test(enabled = false)
public abstract class OFileAbstractTest extends SpeedTestMonoThread {
	protected static final String	FILE_NAME		= "C:/temp/orient-test.file";

	private static final int			NUMS				= 1000000;

	private static final int			START_SIZE	= 100000000;
	private OFile									file;

	protected abstract OFile getFileImpl() throws IOException;

	@Override
	public void init() throws IOException {
		// DELETE THE TEST FILE EVERY TIME
		File f = new File(FILE_NAME);
		if (f.exists())
			f.delete();

		file = getFileImpl();
		file.create(START_SIZE);
	}

	@Override
	public void cycle() throws IOException {
		System.out.println("Writing " + NUMS + " integers...");
		for (int i = 0; i < NUMS; ++i)
			file.writeInt(file.allocateSpace(OBinaryProtocol.SIZE_INT), i);
		data.printSnapshot();

		System.out.println("Checking all written data...");
		for (int i = 0; i < NUMS; ++i)
			Assert.assertTrue(file.readInt(i * OBinaryProtocol.SIZE_INT) == i);
		data.printSnapshot();

		file.close();
	}
}
