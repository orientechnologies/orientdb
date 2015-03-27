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

import com.orientechnologies.orient.etl.OExtractedItem;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.NoSuchElementException;

public class ORowExtractor extends OAbstractSourceExtractor {
  protected BufferedReader bReader;
  protected OExtractedItem next;

  @Override
  public String getName() {
    return "row";
  }

  @Override
  public boolean hasNext() {
    if (next != null)
      return true;

    if (bReader == null)
      return false;

    try {
      next = fetchNext();
      return next != null;
    } catch (IOException e) {
      throw new OExtractorException(e);
    }
  }

  @Override
  public OExtractedItem next() {
    if (next != null) {
      final OExtractedItem ret = next;
      next = null;
      return ret;
    }

    if (!hasNext())
      throw new NoSuchElementException("EOF");

    try {
      return fetchNext();
    } catch (IOException e) {
      throw new OExtractorException(e);
    }
  }

  @Override
  public void extract(final Reader iReader) {
    super.extract(iReader);
    bReader = new BufferedReader(reader);
  }

  @Override
  public void end() {
    if (bReader != null)
      try {
        bReader.close();
      } catch (IOException e) {
      }

    super.end();
  }

  @Override
  public String getUnit() {
    return "rows";
  }

  protected OExtractedItem fetchNext() throws IOException {
    if (!bReader.ready())
      return null;

    final String line = bReader.readLine();

    if( line == null || line.isEmpty())
      return null;

    return new OExtractedItem(current++, line);
  }
}
