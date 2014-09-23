package com.orientechnologies.common.io;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;

/**
 * @author Andrey Lomakin <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 7/22/14
 */
public class OFileUtilsJava7 {
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
