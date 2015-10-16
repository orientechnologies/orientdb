/*
 *
 *  * Copyright 2010-2014 Orient Technologies LTD (info(at)orientechnologies.com)
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

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.etl.OETLProcessor;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.Charset;
import java.util.zip.GZIPInputStream;

public class OFileSource extends OAbstractSource {
  protected String            fileName;
  protected String            path;
  protected boolean           lockFile    = false;
  protected long              byteParsed  = 0;
  protected long              byteToParse = -1;
  protected long              skipFirst   = 0;
  protected long              skipLast    = 0;

  protected RandomAccessFile  raf         = null;
  protected FileChannel       channel     = null;
  protected InputStreamReader fileReader  = null;
  protected FileInputStream   fis         = null;
  protected FileLock          lock        = null;
  private Charset             encoding    = Charset.forName("UTF-8");
  private File                input;

  @Override
  public String getUnit() {
    return "bytes";
  }

  @Override
  public ODocument getConfiguration() {
    return null;
  }

  @Override
  public void configure(OETLProcessor iProcessor, ODocument iConfiguration, OCommandContext iContext) {
    super.configure(iProcessor, iConfiguration, iContext);

    if (iConfiguration.containsField("lock"))
      lockFile = iConfiguration.<Boolean> field("lock");

    if (iConfiguration.containsField("skipFirst"))
      skipFirst = Long.parseLong(iConfiguration.<String> field("skipFirst"));

    if (iConfiguration.containsField("skipLast"))
      skipLast = Long.parseLong(iConfiguration.<String> field("skipLast"));

    if (iConfiguration.containsField("encoding"))
      encoding = Charset.forName(iConfiguration.<String> field("encoding"));

    path = (String) resolve(iConfiguration.field("path"));

    input = new File((String) path);

    if (!input.exists())
      throw new OSourceException("[File source] path '" + path + "' not exists");
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
  public void begin() {

      try {
        final String fileMode = lockFile ? "rw" : "r";
        raf = new RandomAccessFile(input, fileMode);
        channel = raf.getChannel();
        fis = new FileInputStream(input);
        if (fileName.endsWith(".gz"))
          fileReader = new InputStreamReader(new GZIPInputStream(fis),encoding);
        else {
          fileReader = new InputStreamReader(new FileInputStream(input),encoding);
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
        OLogManager.instance().error(this, "Error on locking file: %s", e, fileName);
      }

    log(OETLProcessor.LOG_LEVELS.INFO, "Reading from file " + path + " with encoding " + encoding.displayName());
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
