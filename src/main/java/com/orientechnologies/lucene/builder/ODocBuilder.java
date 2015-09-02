/*
 *
 *  * Copyright 2014 Orient Technologies.
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

package com.orientechnologies.lucene.builder;

import com.orientechnologies.lucene.OLuceneIndexType;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

import java.util.List;

/**
 * Created by Enrico Risa on 02/09/15.
 */
public class ODocBuilder implements DocBuilder {
  @Override
  public Document build(OIndexDefinition definition, Object key, ODocument metadata) {
    Document doc = new Document();
    int i = 0;
    for (String f : definition.getFields()) {
      Object val = null;
      if (key instanceof OCompositeKey) {
        val = ((OCompositeKey) key).getKeys().get(i);

      } else if (key instanceof List) {
        val = ((List) key).get(i);
      } else {
        val = key;
      }
      i++;
      doc.add(OLuceneIndexType.createField(f, val, Field.Store.NO, Field.Index.ANALYZED));
    }
    return doc;
  }
}
