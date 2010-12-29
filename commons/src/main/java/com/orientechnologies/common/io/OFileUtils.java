package com.orientechnologies.common.io;

import java.io.File;

public class OFileUtils {
	private static final int	KILOBYTE	= 1024;
	private static final int	MEGABYTE	= 1048576;
	private static final int	GIGABYTE	= 1073741824;
	private static final long	TERABYTE	= 1099511627776L;

	public static long getSizeAsNumber(String iSize) {
		if (iSize == null)
			throw new IllegalArgumentException("Size " + iSize + " is null");

		boolean number = true;
		for (int i = iSize.length() - 1; i >= 0; --i) {
			if (!Character.isDigit(iSize.charAt(i))) {
				number = false;
				break;
			}
		}

		if (number)
			return Long.parseLong(iSize);
		else {
			iSize = iSize.toUpperCase();
			int pos = iSize.indexOf("KB");
			if (pos > -1)
				return Long.parseLong(iSize.substring(0, pos)) * KILOBYTE;

			pos = iSize.indexOf("MB");
			if (pos > -1)
				return Long.parseLong(iSize.substring(0, pos)) * MEGABYTE;

			pos = iSize.indexOf("GB");
			if (pos > -1)
				return Long.parseLong(iSize.substring(0, pos)) * GIGABYTE;

			pos = iSize.indexOf("TB");
			if (pos > -1)
				return Long.parseLong(iSize.substring(0, pos)) * TERABYTE;

			pos = iSize.indexOf("B");
			if (pos > -1)
				return Long.parseLong(iSize.substring(0, pos));

			pos = iSize.indexOf("%");
			if (pos > -1)
				return -1 * Long.parseLong(iSize.substring(0, pos));

			// RE-THROW THE EXCEPTION
			throw new IllegalArgumentException("Size " + iSize + " has a unrecognizable format");
		}
	}

	public static String getSizeAsString(long iSize) {
		if (iSize > TERABYTE)
			return String.format("%2.2fTb", (float) iSize / TERABYTE);
		if (iSize > GIGABYTE)
			return String.format("%2.2fGb", (float) iSize / GIGABYTE);
		if (iSize > MEGABYTE)
			return String.format("%2.2fMb", (float) iSize / MEGABYTE);
		if (iSize > KILOBYTE)
			return String.format("%2.2fKb", (float) iSize / KILOBYTE);

		return String.valueOf(iSize) + "b";
	}

	public static String getDirectory(String iPath) {
		iPath = getPath(iPath);
		int pos = iPath.lastIndexOf("/");
		if (pos == -1)
			return "";

		return iPath.substring(0, pos);
	}

	public static void createDirectoryTree(final String iFileName) {
		final String[] fileDirectories = iFileName.split("/");
		for (int i = 0; i < fileDirectories.length - 1; ++i)
			new File(fileDirectories[i]).mkdir();
	}

	public static String getPath(final String iPath) {
		if (iPath == null)
			return null;
		return iPath.replace('\\', '/');
	}
}
