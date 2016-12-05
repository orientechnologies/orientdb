/*
  *
  *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
  *  *
  *  *  Licensed under the Apache License, Version 2.0 (the "License");
  *  *  you may not use this file except in compliance with the License.
  *  *  You may obtain a copy of the License at
  *  *
  *  *       http://www.apache.org/licenses/LICENSE-2.0
  *  *
  *  *  Unless required by applicable law or agreed to in writing, software
  *  *  distributed under the License is distributed on an "AS IS" BASIS,
  *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  *  *  See the License for the specific language governing permissions and
  *  *  limitations under the License.
  *  *
  *  * For more information: http://orientdb.com
  *
  */

package com.orientechnologies.common.io;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 7/22/14
 */
public class OFileUtilsJava7 {
	
	private OFileUtilsJava7(){
	}
	
	public static boolean delete(File file) throws IOException {
		if (!file.exists())
			return true;

		try {
			final FileSystem fileSystem = FileSystems.getDefault();
			final Path path = fileSystem.getPath(file.getAbsolutePath());

			Files.delete(path);

			return true;
		} catch (FileSystemException e) {
			return false;
		}
	}
}
