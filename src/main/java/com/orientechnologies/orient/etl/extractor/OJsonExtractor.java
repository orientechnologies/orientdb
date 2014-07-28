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
  protected boolean     collection;

  @Override
  public String getName() {
    return "json";
  }

  @Override
  public boolean hasNext() {
    if (jsonReader == null)
      return false;

    if (collection && jsonReader.lastChar() == ']') {
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
      final String value = jsonReader.readString(OJSONReader.END_OBJECT, true);
      jsonReader.readNext(OJSONReader.NEXT_IN_ARRAY);
      current++;
      return new ODocument().fromJSON(value);
    } catch (Exception e) {
      throw new OExtractorException("[JSON extractor] error on extract json", e);
    }
  }

  @Override
  public void begin() {
    super.begin();
  }

  public void extract(final Reader iReader) {
    super.extract(iReader);
    jsonReader = new OJSONReader(reader);
    try {
      char first = jsonReader.nextChar();
      if (first == '[') {
        collection = true;
      } else if (first == '{') {
        collection = false;
      } else
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
