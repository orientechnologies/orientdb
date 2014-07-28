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

import java.io.Reader;
import java.util.NoSuchElementException;

public class OJsonExtractor extends OAbstractSourceExtractor {
  protected OJSONReader jsonReader;
  protected Character   first = null;

  @Override
  public String getName() {
    return "json";
  }

  @Override
  public boolean hasNext() {
    if (jsonReader == null)
      return false;

    if (total == 1 && jsonReader.lastChar() == ']') {
      jsonReader = null;
      return false;
    }

    try {
      return jsonReader.hasNext();
    } catch (Exception e) {
      throw new OExtractorException("[JSON extractor] error on parsing next element", e);
    }
  }

  @Override
  public Object next() {
    if (!hasNext())
      throw new NoSuchElementException("EOF");

    try {
      String value = jsonReader.readString(OJSONReader.END_OBJECT, true);
      if (first != null) {
        // USE THE FIRST CHAR READ
        value = first + value;
        first = null;
      }
      jsonReader.readNext(OJSONReader.NEXT_IN_ARRAY);
      current++;
      return new ODocument().fromJSON(value);
    } catch (Exception e) {
      throw new OExtractorException("[JSON extractor] error on extract json", e);
    }
  }

  public void extract(final Reader iReader) {
    super.extract(iReader);
    jsonReader = new OJSONReader(reader);
    try {
      first = (char) reader.read();
      if (first == '[')
        first = null;
      else if (first == '{')
        total = 1;
      else
        throw new OExtractorException("[JSON extractor] found unexpected character '" + first + "' at the beginning of input");

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
}
