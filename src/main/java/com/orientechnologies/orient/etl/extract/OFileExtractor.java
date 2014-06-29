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

package com.orientechnologies.orient.etl.extract;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.NoSuchElementException;
import java.util.zip.GZIPInputStream;

public class OFileExtractor extends OAbstractExtractor {
  protected String            fileName;
  protected Object            path;
  protected boolean           lockFile    = false;
  protected long              byteParsed  = 0;
  protected long              byteToParse = -1;

  protected RandomAccessFile  raf         = null;
  protected FileChannel       channel     = null;
  protected InputStreamReader fileReader  = null;
  protected FileInputStream   fis         = null;
  protected FileLock          lock        = null;

  @Override
  public void configure(ODocument iConfiguration) {
    if (iConfiguration.containsField("path"))
      path = iConfiguration.field("path");
    if (iConfiguration.containsField("lock"))
      lockFile = iConfiguration.field("lock");
  }

  @Override
  public String getName() {
    return "file";
  }

  public void extract(OCommandContext context) {
    if (path instanceof String)
      path = new File((String) path);

    if (path instanceof File) {
      final File file = (File) path;
      fileName = file.getName();

      try {
        raf = new RandomAccessFile(file, "rw");
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
      throw new OExtractorException("Unknown input '" + path + "' of class '" + path.getClass() + "'");

    begin();
  }

  @Override
  public boolean hasNext() {
    if (fileReader == null)
      return false;

    try {
      final boolean res = fileReader.ready();
      if (!res)
        // CLOSE IT
        end();
      return res;
    } catch (IOException e) {
      throw new OExtractorException(e);
    }
  }

  @Override
  public Object next() {
    if (!hasNext())
      throw new NoSuchElementException("EOF");

    try {
      byteParsed++;
      return (byte) fileReader.read();
    } catch (IOException e) {
      throw new OExtractorException(e);
    }
  }

  public void end() {
    if (lock != null)
      try {
        lock.release();
      } catch (IOException e) {
        e.printStackTrace();
      }

    if (fis != null)
      try {
        fis.close();
      } catch (IOException e) {
        e.printStackTrace();
      }

    if (fileReader != null)
      try {
        fileReader.close();
      } catch (IOException e) {
        e.printStackTrace();
      }

    if (channel != null)
      try {
        channel.close();
      } catch (IOException e) {
        e.printStackTrace();
      }

    if (raf != null)
      try {
        raf.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
  }

  @Override
  public long getProgress() {
    return byteParsed;
  }

  @Override
  public long getTotal() {
    return byteToParse;
  }

  protected void begin() {
    byteParsed = 0;

    if (lockFile)
      try {
        lock = channel.lock();
      } catch (IOException e) {
        e.printStackTrace();
      }

    final long startTime = System.currentTimeMillis();
  }
}
