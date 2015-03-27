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
import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.etl.OETLProcessor;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.io.Reader;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.zip.GZIPInputStream;

public class OFileSource extends OAbstractSource {
  protected String            fileName;
  protected Object            path;
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

  @Override
  public String getUnit() {
    return "bytes";
  }

  @Override
  public ODocument getConfiguration() {
    return null;
  }

  @Override
  public void configure(OETLProcessor iProcessor, ODocument iConfiguration, OBasicCommandContext iContext) {
    super.configure(iProcessor, iConfiguration, iContext);

    path = resolve(iConfiguration.field("path"));
    if (iConfiguration.containsField("lock"))
      lockFile = (Boolean) iConfiguration.field("lock");

    if (iConfiguration.containsField("skipFirst"))
      skipFirst = Long.parseLong((String) iConfiguration.field("skipFirst"));

    if (iConfiguration.containsField("skipLast"))
      skipLast = Long.parseLong((String) iConfiguration.field("skipLast"));

    if (path instanceof String)
      path = new File((String) path);

    if (path instanceof File) {
      final File file = (File) path;
      if (!file.exists())
        throw new OSourceException("[File source] path '" + path + "' not exists");
      fileName = file.getName();
    }
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
    if (path instanceof File) {
      final File file = (File) path;

      try {
        final String fileMode = lockFile ? "rw" : "r";
        raf = new RandomAccessFile(file, fileMode);
        channel = raf.getChannel();
        fis = new FileInputStream(file);
        if (fileName.endsWith(".gz"))
          fileReader = new InputStreamReader(new GZIPInputStream(fis));
        else {
          fileReader = new FileReader(file);
          byteToParse = file.length();
        }

      } catch (Exception e) {
        end();
      }
    } else if (path instanceof InputStream) {
      fileName = null;
      byteToParse = -1;
      fileReader = new InputStreamReader((InputStream) path);
    } else if (path instanceof InputStreamReader) {
      fileName = null;
      byteToParse = -1;
      fileReader = (InputStreamReader) path;
    } else
      throw new OSourceException("[File source] Unknown input '" + path + "' of class '" + path.getClass() + "'");

    byteParsed = 0;

    if (lockFile)
      try {
        lock = channel.lock();
      } catch (IOException e) {
        OLogManager.instance().error(this, "Error on locking file: %s", e, fileName);
      }

    log(OETLProcessor.LOG_LEVELS.DEBUG, "Reading from file " + path);
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
