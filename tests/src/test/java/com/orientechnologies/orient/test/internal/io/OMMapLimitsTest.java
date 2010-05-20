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
import com.orientechnologies.orient.core.storage.fs.OFile;
import com.orientechnologies.orient.core.storage.fs.OFileMMap;

@Test(enabled = false)
public class OMMapLimitsTest extends SpeedTestMonoThread {
	protected static final String	FILE_NAME		= "C:/temp/orient-test.file";

	private static final int			NUMS				= 20;

	private static final int			START_SIZE	= 100000000;

	@Override
	public void cycle() throws IOException {

		System.out.println("Testing opening of " + NUMS + " mmap files of MB " + START_SIZE / 1000000 + " bytes each...");
		OFile[] files = new OFile[NUMS];
		for (int i = 0; i < NUMS; ++i) {
			// DELETE THE TEST FILE EVERY TIME
			File f = new File(FILE_NAME + i);
			if (f.exists())
				f.delete();

			files[i] = new OFileMMap(FILE_NAME + i, "rw");
			files[i].create(START_SIZE);

			System.out.println("Created file mmap " + (i + 1) + "/" + NUMS + ". Total: " + (((float) (i + 1) * START_SIZE) / 1000000000)
					+ "Gb");
		}
		data.printSnapshot();
	}
}
