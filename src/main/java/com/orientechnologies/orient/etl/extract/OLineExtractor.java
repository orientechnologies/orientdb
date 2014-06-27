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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.NoSuchElementException;

public class OLineExtractor extends OAbstractExtractor {
  protected BufferedReader reader;
  private long             progress;

  @Override
  public String getName() {
    return "line";
  }

  @Override
  public boolean hasNext() {
    if (reader == null)
      return false;

    try {
      final boolean res = reader.ready();
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
      final String line = reader.readLine();
      progress += line.length() + 1;
      return line;
    } catch (IOException e) {
      throw new OExtractorException(e);
    }
  }

  public void end() {
    if (reader != null)
      try {
        reader.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
  }

  @Override
  public void extract(final Object input) {
    if (input instanceof InputStream) {
      reader = new BufferedReader(new InputStreamReader((InputStream) input));
    } else if (input instanceof Reader) {
      reader = new BufferedReader((Reader) input);
    } else
      throw new OExtractorException("Unknown input '" + input + "' of class '" + input.getClass() + "'");

  }

  @Override
  public long getProgress() {
    return progress;
  }

  @Override
  public long getTotal() {
    return -1;
  }
}
