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

package com.orientechnologies.orient.etl.extractor;

import com.orientechnologies.orient.core.record.impl.ODocument;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.NoSuchElementException;

public class OLineExtractor extends OFileExtractor {
  protected BufferedReader reader;
  private long             progressBytes;
  private long             currentRow;

  @Override
  public ODocument getConfiguration() {
    return new ODocument().fromJSON("{parameters:[{path:{optional:false,description:'File path'}},"
        + "{lock:{optional:true,description:'Lock the file while browsing it'}}]," + "output:'String'}");
  }

  @Override
  public String getName() {
    return "line";
  }

  @Override
  public boolean hasNext() {
    if (reader == null)
      return false;

    try {
      return reader.ready();
    } catch (IOException e) {
      throw new OExtractorException(e);
    }
  }

  @Override
  public Object next() {
    if (!hasNext())
      throw new NoSuchElementException("EOF");

    try {
      final String line = reader.readLine();
      progressBytes += line.length();
      currentRow++;
      if (reader.ready())
        progressBytes++;
      return line;
    } catch (IOException e) {
      throw new OExtractorException(e);
    }
  }

  @Override
  public void begin() {
    super.begin();

    reader = new BufferedReader(fileReader);
  }

  public void end() {
    if (reader != null)
      try {
        reader.close();
      } catch (IOException e) {
        e.printStackTrace();
      }

    super.end();
  }

  @Override
  public long getCurrent() {
    return currentRow;
  }

  @Override
  public String getCurrentUnit() {
    return "row";
  }

  @Override
  public long getProgress() {
    return progressBytes;
  }
}
