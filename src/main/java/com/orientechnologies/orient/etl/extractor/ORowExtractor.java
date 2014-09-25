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

  @Override
  public String getName() {
    return "row";
  }

  @Override
  public boolean hasNext() {
    if (bReader == null)
      return false;

    try {
      return bReader.ready();
    } catch (IOException e) {
      throw new OExtractorException(e);
    }
  }

  @Override
  public OExtractedItem next() {
    if (!hasNext())
      throw new NoSuchElementException("EOF");

    try {
      final String line = bReader.readLine();

      return new OExtractedItem(current++, line);

    } catch (IOException e) {
      throw new OExtractorException(e);
    }
  }

  @Override
  public void extract(final Reader iReader) {
    super.extract(iReader);
    bReader = new BufferedReader(reader);
  }

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
}
