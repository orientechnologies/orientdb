/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */

package com.orientechnologies.orient.etl;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.etl.extractor.OETLAbstractExtractor;
import java.io.Reader;
import java.util.Random;

/**
 * ETL .
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com) on 27/11/14.
 */
public class OETLStubRandomExtractor extends OETLAbstractExtractor {
  private long items;
  private int fields;
  private int delay = 0;

  @Override
  public void configure(ODocument conf, OCommandContext ctx) {
    super.configure(conf, ctx);

    if (conf.containsField("items")) {
      items = ((Number) conf.field("items")).longValue();
    }
    if (conf.containsField("fields")) {
      fields = conf.field("fields");
    }
    if (conf.containsField("delay")) {
      delay = conf.field("delay");
    }
  }

  @Override
  public void extract(final Reader iReader) {}

  @Override
  public String getUnit() {
    return "row";
  }

  @Override
  public boolean hasNext() {
    return current < items;
  }

  @Override
  public OETLExtractedItem next() {
    final ODocument doc = new ODocument();

    for (int i = 0; i < fields; ++i) {
      doc.field("field" + i, "value_" + new Random().nextInt(30));
    }

    if (delay > 0) {
      // SIMULATE A SLOW DOWN
      try {
        Thread.sleep(delay);
      } catch (InterruptedException e) {
      }
    }

    return new OETLExtractedItem(current++, doc);
  }

  @Override
  public String getName() {
    return "random";
  }
}
