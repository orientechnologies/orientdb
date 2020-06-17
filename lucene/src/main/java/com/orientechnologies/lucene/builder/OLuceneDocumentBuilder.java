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

import static com.orientechnologies.lucene.builder.OLuceneIndexType.createField;
import static com.orientechnologies.lucene.builder.OLuceneIndexType.createFields;
import static com.orientechnologies.lucene.engine.OLuceneIndexEngineAbstract.RID;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

/** Created by Enrico Risa on 02/09/15. */
public class OLuceneDocumentBuilder {
  public Document newBuild(OIndexDefinition indexDefinition, Object key, OIdentifiable oid) {
    if (oid != null) {
      ORecord record = oid.getRecord();
      OElement element = record.load();
    }
    return null;
  }

  public Document build(
      final OIndexDefinition definition,
      final Object key,
      final OIdentifiable value,
      final Map<String, Boolean> fieldsToStore,
      final ODocument metadata) {
    final Document doc = new Document();
    this.addDefaultFieldsToDocument(definition, value, doc);

    final List<Object> formattedKey = formatKeys(definition, key);
    int counter = 0;
    for (final String field : definition.getFields()) {
      final Object val = formattedKey.get(counter);
      counter++;
      if (val != null) {
        // doc.add(createField(field, val, Field.Store.YES));
        final Boolean sorted = isSorted(field, metadata);
        createFields(field, val, Field.Store.YES, sorted).forEach(f -> doc.add(f));
        // for cross class index
        createFields(definition.getClassName() + "." + field, val, Field.Store.YES, sorted)
            .forEach(f -> doc.add(f));
      }
    }
    return doc;
  }

  private void addDefaultFieldsToDocument(
      OIndexDefinition definition, OIdentifiable value, Document doc) {
    if (value != null) {
      doc.add(createField(RID, value.getIdentity().toString(), Field.Store.YES));
      doc.add(createField("_CLUSTER", "" + value.getIdentity().getClusterId(), Field.Store.YES));
      doc.add(createField("_CLASS", definition.getClassName(), Field.Store.YES));
    }
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

  protected Boolean isSorted(String field, ODocument metadata) {
    if (metadata == null) return true;
    Boolean sorted = true;
    try {
      Boolean localSorted = metadata.field("*_index_sorted");
      if (localSorted == null) {
        localSorted = metadata.field(field + "_index_sorted");
      }
      if (localSorted != null) {
        sorted = localSorted;
      }
    } catch (Exception e) {
    }
    return sorted;
  }
}
