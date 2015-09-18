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

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.etl.OETLProcessor;

import java.io.BufferedReader;
import java.io.Reader;
import java.io.StringReader;

/**
 * ETL Source created with a string content.
 */
public class OContentSource extends OAbstractSource {
  protected BufferedReader reader;

  @Override
  public void configure(OETLProcessor iProcessor, ODocument iConfiguration, OCommandContext iContext) {
    final Object value = iConfiguration.field("value");
    if (value != null) {
      String stringContent;
      if (value instanceof ODocument)
        stringContent = ((ODocument) value).toJSON(null);
      else if (OMultiValue.isMultiValue(value)) {
        stringContent = "[";
        int i = 0;
        for (Object o : OMultiValue.getMultiValueIterable(value)) {
          if (o != null) {
            if (i > 0)
              stringContent += ",";

            if (o instanceof ODocument)
              stringContent += ((ODocument) o).toJSON(null);
            else
              stringContent += o.toString();
            ++i;
          }
        }
        stringContent += "]";
      } else
        stringContent = value.toString();

      this.reader = new BufferedReader(new StringReader(stringContent));
    } else
      throw new IllegalArgumentException(getName() + " Source has no 'value' set");
  }

  @Override
  public ODocument getConfiguration() {
    return new ODocument().fromJSON("{}");
  }

  @Override
  public String getUnit() {
    return "bytes";
  }

  @Override
  public String getName() {
    return "content";
  }

  @Override
  public Reader read() {
    return reader;
  }
}
