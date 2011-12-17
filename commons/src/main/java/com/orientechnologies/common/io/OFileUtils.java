package com.orientechnologies.common.io;

import java.io.File;
import java.util.Locale;

public class OFileUtils {
	private static final int	KILOBYTE	= 1024;
	private static final int	MEGABYTE	= 1048576;
	private static final int	GIGABYTE	= 1073741824;
	private static final long	TERABYTE	= 1099511627776L;

	@SuppressWarnings("unchecked")
	public static long getSizeAsNumber(final Object iSize) {
		if (iSize == null)
			throw new IllegalArgumentException("Size is null");

		if (iSize instanceof Number)
			return ((Number) iSize).longValue();

		String size = iSize.toString();

		boolean number = true;
		for (int i = size.length() - 1; i >= 0; --i) {
			if (!Character.isDigit(size.charAt(i))) {
				number = false;
				break;
			}
		}

		if (number)
			return string2number(size).longValue();
		else {
			size = size.toUpperCase(Locale.ENGLISH);
			int pos = size.indexOf("KB");
			if (pos > -1)
				return (long) (string2number(size.substring(0, pos)).floatValue() * KILOBYTE);

			pos = size.indexOf("MB");
			if (pos > -1)
				return (long) (string2number(size.substring(0, pos)).floatValue() * MEGABYTE);

			pos = size.indexOf("GB");
			if (pos > -1)
				return (long) (string2number(size.substring(0, pos)).floatValue() * GIGABYTE);

			pos = size.indexOf("TB");
			if (pos > -1)
				return (long) (string2number(size.substring(0, pos)).floatValue() * TERABYTE);

			pos = size.indexOf('B');
			if (pos > -1)
				return (long) string2number(size.substring(0, pos)).floatValue();

			pos = size.indexOf('%');
			if (pos > -1)
				return (long) (-1 * string2number(size.substring(0, pos)).floatValue());

			// RE-THROW THE EXCEPTION
			throw new IllegalArgumentException("Size " + size + " has a unrecognizable format");
		}
	}

	public static Number string2number(final String iText) {
		if (iText.indexOf('.') > -1)
			return Double.parseDouble(iText);
		else
			return Long.parseLong(iText);
	}

	public static String getSizeAsString(final long iSize) {
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
