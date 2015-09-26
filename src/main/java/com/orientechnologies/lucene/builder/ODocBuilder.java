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
import com.orientechnologies.lucene.engine.OLuceneIndexEngineAbstract;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Enrico Risa on 02/09/15.
 */
public class ODocBuilder implements DocBuilder {
  @Override
  public Document build(OIndexDefinition definition, Object key, OIdentifiable value, ODocument metadata) {
    Document doc = new Document();

    if (value != null) {
      doc.add(OLuceneIndexType.createField(OLuceneIndexEngineAbstract.RID, value.getIdentity().toString(), Field.Store.YES,
          Field.Index.NOT_ANALYZED_NO_NORMS));
    }
    List<Object> formattedKey = formatKeys(definition, key);

    int i = 0;
    for (String f : definition.getFields()) {
      Object val = formattedKey.get(i);
      i++;
      doc.add(OLuceneIndexType.createField(f, val, Field.Store.NO, Field.Index.ANALYZED));
    }
    return doc;
  }

  private List<Object> formatKeys(OIndexDefinition definition, Object key) {
    List<Object> keys;

    if (key instanceof OCompositeKey) {
      keys = ((OCompositeKey) key).getKeys();

    } else if (key instanceof List) {
      keys = ((List) key);
    } else {
      keys = new ArrayList<Object>();
      keys.add(key);
    }

    //a sort of pa
    for (int i = keys.size(); i < definition.getFields().size(); i++) {
      keys.add("");
    }
    return keys;
  }
}
