/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.sql.functions.coll;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * This operator add an entry in a map. The entry is composed by a key and a value.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OSQLFunctionDocument extends OSQLFunctionMultiValueAbstract<ODocument> {
  public static final String NAME = "document";

  public OSQLFunctionDocument() {
    super(NAME, 1, -1);
  }

  @SuppressWarnings("unchecked")
  public Object execute(final OIdentifiable iCurrentRecord, Object iCurrentResult, final Object[] iParameters,
      OCommandContext iContext) {

    if (iParameters.length > 2)
      // IN LINE MODE
      context = new ODocument();

    if (iParameters.length == 1) {
      if (iParameters[0] instanceof ODocument)
        // INSERT EVERY DOCUMENT FIELD
        context.merge((ODocument) iParameters[0], true, false);
      else if (iParameters[0] instanceof Map<?, ?>)
        // INSERT EVERY SINGLE COLLECTION ITEM
        context.fields((Map<String, Object>) iParameters[0]);
      else
        throw new IllegalArgumentException("Map function: expected a map or pairs of parameters as key, value");
    } else if (iParameters.length % 2 != 0)
      throw new IllegalArgumentException("Map function: expected a map or pairs of parameters as key, value");
    else
      for (int i = 0; i < iParameters.length; i += 2) {
        final String key = iParameters[i].toString();
        final Object value = iParameters[i + 1];

        if (value != null) {
          if (iParameters.length <= 2 && context == null)
            // AGGREGATION MODE (STATEFULL)
            context = new ODocument();

          context.field(key, value);
        }
      }

    return prepareResult(context);
  }

  public String getSyntax() {
    return "Syntax error: map(<map>|[<key>,<value>]*)";
  }

  public boolean aggregateResults(final Object[] configuredParameters) {
    return configuredParameters.length <= 2;
  }

  @Override
  public ODocument getResult() {
    final ODocument res = context;
    context = null;
    return prepareResult(res);
  }

  protected ODocument prepareResult(ODocument res) {
    if (returnDistributedResult()) {
      final ODocument doc = new ODocument();
      doc.field("node", getDistributedStorageId());
      doc.field("context", res);
      return doc;
    } else {
      return res;
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public Object mergeDistributedResult(List<Object> resultsToMerge) {
    final Map<Long, Map<Object, Object>> chunks = new HashMap<Long, Map<Object, Object>>();
    for (Object iParameter : resultsToMerge) {
      final Map<String, Object> container = (Map<String, Object>) ((Map<Object, Object>) iParameter).get("doc");
      chunks.put((Long) container.get("node"), (Map<Object, Object>) container.get("context"));
    }
    final Map<Object, Object> result = new HashMap<Object, Object>();
    for (Map<Object, Object> chunk : chunks.values()) {
      result.putAll(chunk);
    }
    return result;
  }
}
