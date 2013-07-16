/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.common.io;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Locale;

public class OFileUtils {
  private static final int  KILOBYTE = 1024;
  private static final int  MEGABYTE = 1048576;
  private static final int  GIGABYTE = 1073741824;
  private static final long TERABYTE = 1099511627776L;

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

  public static void checkValidName(final String iFileName) throws IOException {
    if (iFileName.contains("..") || iFileName.contains("/") || iFileName.contains("\\"))
      throw new IOException("Invalid file name '" + iFileName + "'");
  }

  public static void deleteRecursively(final File iRootFile) {
    if (iRootFile.exists()) {
      if (iRootFile.isDirectory()) {
        for (File f : iRootFile.listFiles()) {
          if (f.isFile())
            f.delete();
          else
            deleteRecursively(f);
        }
      }
      iRootFile.delete();
    }
  }

  @SuppressWarnings("resource")
  public static final void copyFile(final File source, final File destination) throws IOException {
    FileChannel sourceChannel = new FileInputStream(source).getChannel();
    FileChannel targetChannel = new FileOutputStream(destination).getChannel();
    sourceChannel.transferTo(0, sourceChannel.size(), targetChannel);
    sourceChannel.close();
    targetChannel.close();
  }

  public static final void copyDirectory(final File source, final File destination) throws IOException {
    if (!destination.exists())
      destination.mkdirs();

    for (File f : source.listFiles()) {
      final File target = new File(destination.getAbsolutePath() + "/" + f.getName());
      if (f.isFile())
        copyFile(f, target);
      else
        copyDirectory(f, target);
    }
  }
}
