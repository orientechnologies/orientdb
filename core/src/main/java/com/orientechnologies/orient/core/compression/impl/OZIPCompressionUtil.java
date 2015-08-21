/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientechnologies.com
 *
 */

package com.orientechnologies.orient.core.compression.impl;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.command.OCommandOutputListener;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

/**
 * Compression Utility.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class OZIPCompressionUtil {
  public static List<String> compressDirectory(final String sourceFolderName, final OutputStream output,
      final String[] iSkipFileExtensions, final OCommandOutputListener iOutput, int compressionLevel) throws IOException {

    final List<String> compressedFiles = new ArrayList<String>();

    final ZipOutputStream zos = new ZipOutputStream(output);
    zos.setComment("OrientDB Backup executed on " + new Date());
    try {
      zos.setLevel(compressionLevel);
      addFolder(zos, sourceFolderName, sourceFolderName, iSkipFileExtensions, iOutput, compressedFiles);

      return compressedFiles;
    } finally {
      zos.close();
    }
  }

  /***
   * Extract zipfile to outdir with complete directory structure
   */
  public static void uncompressDirectory(final InputStream in, final String out, final OCommandOutputListener iListener)
      throws IOException {
    final File outdir = new File(out);
    final ZipInputStream zin = new ZipInputStream(in);
    try {
      ZipEntry entry;
      String name, dir;
      while ((entry = zin.getNextEntry()) != null) {
        name = entry.getName();
        if (entry.isDirectory()) {
          mkdirs(outdir, name);
          continue;
        }
        /*
         * this part is necessary because file entry can come before directory entry where is file located i.e.: /foo/foo.txt /foo/
         */
        dir = getDirectoryPart(name);
        if (dir != null)
          mkdirs(outdir, dir);

        extractFile(zin, outdir, name, iListener);
      }
    } finally {
      zin.close();
    }
  }

  private static void extractFile(final ZipInputStream in, final File outdir, final String name,
      final OCommandOutputListener iListener) throws IOException {
    if (iListener != null)
      iListener.onMessage("\n- Uncompressing file " + name + "...");

    final BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(new File(outdir, name)));
    try {
      OIOUtils.copyStream(in, out, -1);
    } finally {
      out.close();
    }
  }

  private static void mkdirs(final File outdir, final String path) {
    final File d = new File(outdir, path);
    if (!d.exists())
      d.mkdirs();
  }

  private static String getDirectoryPart(final String name) {
    final int s = name.lastIndexOf(File.separatorChar);
    return s == -1 ? null : name.substring(0, s);
  }

  private static void addFolder(ZipOutputStream zos, String path, String baseFolderName, final String[] iSkipFileExtensions,
      final OCommandOutputListener iOutput, final List<String> iCompressedFiles) throws IOException {

    File f = new File(path);
    if (f.exists()) {
      if (f.isDirectory()) {
        File f2[] = f.listFiles();
        for (int i = 0; i < f2.length; i++) {
          addFolder(zos, f2[i].getAbsolutePath(), baseFolderName, iSkipFileExtensions, iOutput, iCompressedFiles);
        }
      } else {
        // add file
        // extract the relative name for entry purpose
        String entryName = path.substring(baseFolderName.length() + 1, path.length());

        if (iSkipFileExtensions != null)
          for (String skip : iSkipFileExtensions)
            if (entryName.endsWith(skip))
              return;

        iCompressedFiles.add(path);

        addFile(zos, path, entryName, iOutput);
      }

    } else {
      throw new IllegalArgumentException("Directory " + path + " not found");
    }
  }

  public static void compressFile(final String folderName, final String entryName, final OutputStream output,
      final OCommandOutputListener iOutput, final int compressionLevel) throws IOException {
    final ZipOutputStream zos = new ZipOutputStream(output);
    zos.setComment("OrientDB Backup executed on " + new Date());
    try {
      zos.setLevel(compressionLevel);
      addFile(zos, folderName + "/" + entryName, entryName, iOutput);
    } finally {
      zos.close();
    }
  }

  public static void compressFiles(final String folderName, final String[] entryNames, final OutputStream output,
      final OCommandOutputListener iOutput, final int compressionLevel) throws IOException {
    final ZipOutputStream zos = new ZipOutputStream(output);
    zos.setComment("OrientDB Backup executed on " + new Date());
    try {
      zos.setLevel(compressionLevel);
      for (String entryName : entryNames)
        addFile(zos, folderName + "/" + entryName, entryName, iOutput);
    } finally {
      zos.close();
    }
  }

  private static void addFile(final ZipOutputStream zos, final String folderName, final String entryName,
      final OCommandOutputListener iOutput) throws IOException {
    final long begin = System.currentTimeMillis();

    if (iOutput != null)
      iOutput.onMessage("\n- Compressing file " + entryName + "...");

    final ZipEntry ze = new ZipEntry(entryName);
    zos.putNextEntry(ze);
    try {
      final FileInputStream in = new FileInputStream(folderName);
      try {
        OIOUtils.copyStream(in, zos, -1);
      } finally {
        in.close();
      }
    } catch (IOException e) {
      if (iOutput != null)
        iOutput.onMessage("error: " + e);

      OLogManager.instance().error(OZIPCompression.class, "Cannot compress file: %s", e, folderName);
      throw e;
    } finally {
      zos.closeEntry();
    }

    if (iOutput != null) {
      final long ratio = ze.getSize() > 0 ? 100 - (ze.getCompressedSize() * 100 / ze.getSize()) : 0;

      iOutput.onMessage("ok size=" + OFileUtils.getSizeAsString(ze.getSize()) + " compressedSize=" + ze.getCompressedSize()
          + " ratio=" + ratio + "%% elapsed=" + OIOUtils.getTimeAsString(System.currentTimeMillis() - begin) + "");
    }
  }
}
