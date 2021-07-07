/*
 *
 *  * Copyright 2010-2016 OrientDB LTD (info(-at-)orientdb.com)
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.orientechnologies.orient.etl.source;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.io.Reader;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.Charset;
import java.util.logging.Level;
import java.util.zip.GZIPInputStream;
import org.apache.commons.io.input.BOMInputStream;

public class OETLFileSource extends OETLAbstractSource {
  protected String fileName;
  protected String path;
  protected boolean lockFile = false;
  protected boolean skipBOM = true;
  protected long byteParsed = 0;
  protected long byteToParse = -1;
  protected long skipFirst = 0;
  protected long skipLast = 0;
  protected RandomAccessFile raf = null;
  protected FileChannel channel = null;
  protected InputStreamReader fileReader = null;
  protected InputStream fis = null;
  protected FileLock lock = null;
  private Charset encoding = Charset.forName("UTF-8");
  private File input;

  @Override
  public String getUnit() {
    return "bytes";
  }

  @Override
  public ODocument getConfiguration() {
    return null;
  }

  @Override
  public void configure(ODocument iConfiguration, OCommandContext iContext) {
    super.configure(iConfiguration, iContext);

    if (iConfiguration.containsField("lock")) lockFile = iConfiguration.<Boolean>field("lock");

    if (iConfiguration.containsField("skipBOM"))
      skipBOM = iConfiguration.field("skipBOM", Boolean.class);

    if (iConfiguration.containsField("skipFirst"))
      skipFirst = Long.parseLong(iConfiguration.<String>field("skipFirst"));

    if (iConfiguration.containsField("skipLast"))
      skipLast = Long.parseLong(iConfiguration.<String>field("skipLast"));

    if (iConfiguration.containsField("encoding"))
      encoding = Charset.forName(iConfiguration.<String>field("encoding"));

    path = (String) resolve(iConfiguration.field("path"));

    input = new File((String) path);

    if (!input.exists())
      throw new OETLSourceException("[File source] path '" + path + "' not exists");
    fileName = input.getName();
  }

  @Override
  public void end() {
    if (lock != null)
      try {
        lock.release();
      } catch (IOException e) {
      }

    if (fis != null)
      try {
        fis.close();
      } catch (IOException e) {
      }

    if (fileReader != null)
      try {
        fileReader.close();
      } catch (IOException e) {
      }

    if (channel != null)
      try {
        channel.close();
      } catch (IOException e) {
      }

    if (raf != null)
      try {
        raf.close();
      } catch (IOException e) {
      }
  }

  @Override
  public String getName() {
    return "file";
  }

  @Override
  public void begin(ODatabaseDocument db) {

    try {
      final String fileMode = lockFile ? "rw" : "r";
      raf = new RandomAccessFile(input, fileMode);
      channel = raf.getChannel();
      fis = new FileInputStream(input);
      if (skipBOM) fis = new BOMInputStream(fis);
      if (fileName.endsWith(".gz"))
        fileReader = new InputStreamReader(new GZIPInputStream(fis), encoding);
      else {
        fileReader = new InputStreamReader(fis, encoding);
        byteToParse = input.length();
      }

    } catch (Exception e) {
      end();
    }

    byteParsed = 0;

    if (lockFile)
      try {
        lock = channel.lock();
      } catch (IOException e) {
        getContext().getMessageHandler().error(this, "Error on locking file: %s", e, fileName);
      }

    log(Level.INFO, "Reading from file " + path + " with encoding " + encoding.displayName());
  }

  public boolean isClosed() {
    return fileReader != null;
  }

  public Reader getFileReader() {
    return fileReader;
  }

  @Override
  public Reader read() {
    return fileReader;
  }
}
