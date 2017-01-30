/*
 *
 *  * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.orientechnologies.lucene.OLuceneIndexType.createField;
import static com.orientechnologies.lucene.engine.OLuceneIndexEngineAbstract.RID;

/**
 * Created by Enrico Risa on 02/09/15.
 */
public class OLuceneDocumentBuilder {

  public Document build(OIndexDefinition definition, Object key, OIdentifiable value, Map<String, Boolean> fieldsToStore,
      ODocument metadata) {
    Document doc = new Document();

    if (value != null) {
      doc.add(createField(RID, value.getIdentity().toString(), Field.Store.YES));
      doc.add(createField("_CLUSTER", "" + value.getIdentity().getClusterId(), Field.Store.YES));
      doc.add(createField("_CLASS", definition.getClassName(), Field.Store.YES));

    }
    List<Object> formattedKey = formatKeys(definition, key);

    int i = 0;
    for (String field : definition.getFields()) {
      Object val = formattedKey.get(i);
      i++;
      if (val != null) {
        doc.add(createField(field, val, Field.Store.YES));
        //for cross class index
        doc.add(createField(definition.getClassName() + "." + field, val, Field.Store.YES));
      }
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

    // a sort of padding
    for (int i = keys.size(); i < definition.getFields().size(); i++) {
      keys.add("");
    }
    return keys;
  }

  protected Field.Store isToStore(String f, Map<String, Boolean> collectionFields) {
    return collectionFields.get(f) ? Field.Store.YES : Field.Store.NO;
  }
}
