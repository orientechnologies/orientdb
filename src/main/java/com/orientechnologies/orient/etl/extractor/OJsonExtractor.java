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
import com.orientechnologies.orient.core.serialization.serializer.OJSONReader;
import com.orientechnologies.orient.etl.OExtractedItem;

import java.io.IOException;
import java.io.Reader;
import java.text.ParseException;
import java.util.NoSuchElementException;

public class OJsonExtractor extends OAbstractSourceExtractor {
  protected OJSONReader    jsonReader;
  protected Character      first = null;
  protected OExtractedItem next;

  @Override
  public String getName() {
    return "json";
  }

  @Override
  public boolean hasNext() {
    if (next != null)
      return true;

    if (jsonReader == null)
      return false;

    try {
      next = fetchNext();
      return next != null;
    } catch (Exception e) {
      throw new OExtractorException("[JSON extractor] error on extract json", e);
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

    } catch (Exception e) {
      throw new OExtractorException("[JSON extractor] error on extract json", e);
    }
  }

  @Override
  public void extract(final Reader iReader) {
    super.extract(iReader);
    try {
      final int read = reader.read();
      if (read == -1)
        return;

      first = (char) read;
      if (first == '[')
        first = null;
      else if (first == '{')
        total = 1;
      else
        throw new OExtractorException("[JSON extractor] found unexpected character '" + first + "' at the beginning of input");

      jsonReader = new OJSONReader(reader);

    } catch (Exception e) {
      throw new OExtractorException(e);
    }
  }

  @Override
  public ODocument getConfiguration() {
    return new ODocument().fromJSON("{parameters:[],output:'ODocument'}");
  }

  @Override
  public String getUnit() {
    return "entries";
  }

  protected OExtractedItem fetchNext() throws IOException, ParseException {
    if (!jsonReader.hasNext())
      return null;

    String value = jsonReader.readString(new char[] { '}', ']' }, true);
    if (first != null) {
      // USE THE FIRST CHAR READ
      value = first + value;
      first = null;
    }

    if (total == 1 && jsonReader.lastChar() == '}') {
      jsonReader = null;
    } else if (total != 1 && jsonReader.lastChar() == ']') {
      if (!value.isEmpty())
        value = value.substring(0, value.length() - 1);
      jsonReader = null;
    } else {
      jsonReader.readNext(OJSONReader.NEXT_IN_ARRAY);
      if (jsonReader.lastChar() == ']')
        jsonReader = null;
    }

    value = value.trim();

    if (value.isEmpty())
      return null;

    return new OExtractedItem(current++, new ODocument().fromJSON(value));
  }
}
