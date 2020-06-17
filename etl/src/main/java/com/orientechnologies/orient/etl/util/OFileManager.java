/*
 *
 *  *  Copyright 2010-2017 OrientDB LTD (http://orientdb.com)
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

package com.orientechnologies.orient.etl.util;

import com.orientechnologies.orient.core.record.impl.ODocument;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Reader;
import java.nio.charset.Charset;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class OFileManager {

  public static void deleteResource(String resourcePath) throws IOException {

    File currentFile = new File(resourcePath);
    if (currentFile.exists()) {
      if (currentFile.isDirectory()) {
        File[] innerFiles = currentFile.listFiles();
        for (File file : innerFiles) {
          deleteResource(file.getCanonicalPath());
        }
        currentFile.delete();
      } else {
        if (!currentFile.delete()) throw new IOException();
      }
    }
  }

  public static void extractAll(String inputArchiveFilePath, String outputFolderPath)
      throws IOException {

    File inputArchiveFile = new File(inputArchiveFilePath);
    if (!inputArchiveFile.exists()) {
      throw new IOException(inputArchiveFile.getAbsolutePath() + " does not exist");
    }

    // distinguishing archive format
    //		TODO

    // Extracting zip file
    unZipAll(inputArchiveFile, outputFolderPath);
  }

  public static void unZipAll(File inputZipFile, String destinationFolderPath) throws IOException {

    byte[] buffer = new byte[1024];

    // get the zip file content
    ZipInputStream zipInputStream = new ZipInputStream(new FileInputStream(inputZipFile));
    // get the zipped file list entry
    ZipEntry zipEntry = zipInputStream.getNextEntry();

    while (zipEntry != null) {

      String fileName = zipEntry.getName();
      String newFilePath = destinationFolderPath + File.separator + fileName;
      File newFile = new File(newFilePath);

      FileOutputStream fileOutputStream = null;

      // if the entry is a file, extracts it
      if (!zipEntry.isDirectory()) {
        fileOutputStream = new FileOutputStream(newFile);
        int len;
        while ((len = zipInputStream.read(buffer)) > 0) {
          fileOutputStream.write(buffer, 0, len);
        }

      } else {
        // if the entry is a directory, make the directory
        File dir = new File(newFilePath);
        dir.mkdir();
      }

      if (fileOutputStream != null) fileOutputStream.close();
      zipEntry = zipInputStream.getNextEntry();
    }

    zipInputStream.closeEntry();
    zipInputStream.close();
  }

  public static String readAllTextFile(Reader rd) throws IOException {
    StringBuilder sb = new StringBuilder();
    int cp;
    while ((cp = rd.read()) != -1) {
      sb.append((char) cp);
    }
    return sb.toString();
  }

  /**
   * It returns a ODocument starting from a json file.
   *
   * @param filePath
   * @return ODocument (null if the file does not exist or problem are encountered during the
   *     reading)
   */
  public static ODocument buildJsonFromFile(String filePath) throws IOException {

    if (filePath == null) {
      return null;
    }

    File jsonFile = new File(filePath);
    if (!jsonFile.exists()) {
      return null;
    }

    FileInputStream is = new FileInputStream(jsonFile);
    BufferedReader rd = new BufferedReader(new InputStreamReader(is, Charset.forName("UTF-8")));
    ODocument json = new ODocument();
    String jsonText = OFileManager.readAllTextFile(rd);
    json.fromJSON(jsonText, "noMap");
    return json;
  }

  public static void writeFileFromText(String text, String outFilePath, boolean append)
      throws IOException {

    File outFile = new File(outFilePath);
    outFile.getParentFile().mkdirs();
    outFile.createNewFile();
    PrintWriter out = new PrintWriter(new FileWriter(outFile, append));
    out.println(text);
    out.close();
  }
}
