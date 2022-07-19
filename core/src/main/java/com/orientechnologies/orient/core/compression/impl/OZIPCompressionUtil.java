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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

/**
 * Compression Utility.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OZIPCompressionUtil {
  public static List<String> compressDirectory(
      final String sourceFolderName,
      final ZipOutputStream zos,
      final String[] iSkipFileExtensions,
      final OCommandOutputListener iOutput)
      throws IOException {
    final List<String> compressedFiles = new ArrayList<>();
    addFolder(
        zos, sourceFolderName, sourceFolderName, iSkipFileExtensions, iOutput, compressedFiles);
    return compressedFiles;
  }

  /** * Extract zipfile to outdir with complete directory structure */
  public static void uncompressDirectory(
      final InputStream in, final String out, final OCommandOutputListener iListener)
      throws IOException {
    final File outdir = new File(out);
    final String targetDirPath = outdir.getCanonicalPath() + File.separator;

    try (ZipInputStream zin = new ZipInputStream(in)) {
      ZipEntry entry;
      String name;
      String dir;
      while ((entry = zin.getNextEntry()) != null) {
        name = entry.getName();

        final File file = new File(outdir, name);
        if (!file.getCanonicalPath().startsWith(targetDirPath))
          throw new IOException(
              "Expanding '"
                  + entry.getName()
                  + "' would create file outside of directory '"
                  + outdir
                  + "'");

        if (entry.isDirectory()) {
          mkdirs(outdir, name);
          continue;
        }

        /*
         * this part is necessary because file entry can come before directory entry where is file located i.e.: /foo/foo.txt /foo/
         */
        dir = getDirectoryPart(name);
        if (dir != null) mkdirs(outdir, dir);

        extractFile(zin, outdir, name, iListener);
      }
    }
  }

  private static void extractFile(
      final ZipInputStream in,
      final File outdir,
      final String name,
      final OCommandOutputListener iListener)
      throws IOException {
    if (iListener != null) iListener.onMessage("\n- Uncompressing file " + name + "...");

    try (BufferedOutputStream out =
        new BufferedOutputStream(new FileOutputStream(new File(outdir, name)))) {
      OIOUtils.copyStream(in, out);
    }
  }

  private static void mkdirs(final File outdir, final String path) {
    final File d = new File(outdir, path);
    if (!d.exists()) d.mkdirs();
  }

  private static String getDirectoryPart(final String name) {
    Path path = Paths.get(name);
    Path parent = path.getParent();
    if (parent != null) return parent.toString();

    return null;
  }

  private static void addFolder(
      ZipOutputStream zos,
      String path,
      String baseFolderName,
      final String[] iSkipFileExtensions,
      final OCommandOutputListener iOutput,
      final List<String> iCompressedFiles)
      throws IOException {

    File f = new File(path);
    if (!f.exists()) {
      String entryName = path.substring(baseFolderName.length() + 1);
      for (String skip : iSkipFileExtensions) {
        if (entryName.endsWith(skip)) {
          return;
        }
      }
    }
    if (f.exists()) {
      if (f.isDirectory()) {
        final File[] files = f.listFiles();
        if (files != null) {
          for (File file : files) {
            addFolder(
                zos,
                file.getAbsolutePath(),
                baseFolderName,
                iSkipFileExtensions,
                iOutput,
                iCompressedFiles);
          }
        }
      } else {
        // add file
        // extract the relative name for entry purpose
        String entryName = path.substring(baseFolderName.length() + 1);

        if (iSkipFileExtensions != null)
          for (String skip : iSkipFileExtensions)
            if (entryName.endsWith(skip)) {
              return;
            }

        iCompressedFiles.add(path);

        addFile(zos, path, entryName, iOutput);
      }

    } else {
      throw new IllegalArgumentException("Directory " + path + " not found");
    }
  }

  /**
   * Compresses the given files stored at the given base directory into a zip archive.
   *
   * @param baseDirectory the base directory where files are stored.
   * @param fileNames the file names map, keys are the file names stored on disk, values are the
   *     file names to be stored in a zip archive.
   * @param output the output stream.
   * @param listener the command listener.
   * @param compressionLevel the desired compression level.
   */
  public static void compressFiles(
      String baseDirectory,
      Map<String, String> fileNames,
      OutputStream output,
      OCommandOutputListener listener,
      int compressionLevel)
      throws IOException {
    final ZipOutputStream zipOutputStream = new ZipOutputStream(output);
    zipOutputStream.setComment("OrientDB Backup executed on " + new Date());
    try {
      zipOutputStream.setLevel(compressionLevel);
      for (Map.Entry<String, String> entry : fileNames.entrySet())
        addFile(zipOutputStream, baseDirectory + "/" + entry.getKey(), entry.getValue(), listener);
    } finally {
      zipOutputStream.close();
    }
  }

  private static void addFile(
      final ZipOutputStream zos,
      final String folderName,
      final String entryName,
      final OCommandOutputListener iOutput)
      throws IOException {
    final long begin = System.currentTimeMillis();

    if (iOutput != null) iOutput.onMessage("\n- Compressing file " + entryName + "...");

    final ZipEntry ze = new ZipEntry(entryName);
    zos.putNextEntry(ze);
    try {
      final FileInputStream in = new FileInputStream(folderName);
      try {
        OIOUtils.copyStream(in, zos);
      } finally {
        in.close();
      }
    } catch (IOException e) {
      if (iOutput != null) iOutput.onMessage("error: " + e);

      OLogManager.instance()
          .error(OZIPCompressionUtil.class, "Cannot compress file: %s", e, folderName);
      throw e;
    } finally {
      zos.closeEntry();
    }

    if (iOutput != null) {
      final long ratio = ze.getSize() > 0 ? 100 - (ze.getCompressedSize() * 100 / ze.getSize()) : 0;

      iOutput.onMessage(
          "ok size="
              + OFileUtils.getSizeAsString(ze.getSize())
              + " compressedSize="
              + ze.getCompressedSize()
              + " ratio="
              + ratio
              + "%% elapsed="
              + OIOUtils.getTimeAsString(System.currentTimeMillis() - begin)
              + "");
    }
  }
}
