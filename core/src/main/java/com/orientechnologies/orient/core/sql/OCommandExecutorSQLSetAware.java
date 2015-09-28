/*
 *
 * Copyright 2012 Luca Molino (molino.luca--AT--gmail.com)
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
package com.orientechnologies.orient.core.sql;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author luca.molino
 * 
 */
public abstract class OCommandExecutorSQLSetAware extends OCommandExecutorSQLAbstract {

  protected static final String KEYWORD_SET      = "SET";
  protected static final String KEYWORD_CONTENT  = "CONTENT";

  protected ODocument           content          = null;
  protected int                 parameterCounter = 0;

  protected void parseContent() {
    if (!parserIsEnded() && !parserGetLastWord().equals(KEYWORD_WHERE))
      content = parseJSON();

    if (content == null)
      throwSyntaxErrorException("Content not provided. Example: CONTENT { \"name\": \"Jay\" }");
  }

  protected void parseSetFields(final OClass iClass, final List<OPair<String, Object>> fields) {
    String fieldName;
    String fieldValue;

    while (!parserIsEnded() && (fields.size() == 0 || parserGetLastSeparator() == ',' || parserGetCurrentChar() == ',')) {
      fieldName = parserRequiredWord(false, "Field name expected");
      if (fieldName.equalsIgnoreCase(KEYWORD_WHERE)) {
        parserGoBack();
        break;
      }

      parserNextChars(false, true, "=");
      fieldValue = parserRequiredWord(false, "Value expected", " =><,\r\n");

      // INSERT TRANSFORMED FIELD VALUE
      final Object v = convertValue(iClass, fieldName, getFieldValueCountingParameters(fieldValue));

      fields.add(new OPair(fieldName, v));
      parserSkipWhiteSpaces();
    }

    if (fields.size() == 0)
      throwParsingException("Entries to set <field> = <value> are missed. Example: name = 'Bill', salary = 300.2");
  }

  protected OClass extractClassFromTarget(String iTarget) {
    // CLASS
    if (!iTarget.startsWith(OCommandExecutorSQLAbstract.CLUSTER_PREFIX)
        && !iTarget.startsWith(OCommandExecutorSQLAbstract.INDEX_PREFIX)) {

      if (iTarget.startsWith(OCommandExecutorSQLAbstract.CLASS_PREFIX))
        // REMOVE CLASS PREFIX
        iTarget = iTarget.substring(OCommandExecutorSQLAbstract.CLASS_PREFIX.length());

      if (iTarget.charAt(0) == ORID.PREFIX)
        return getDatabase().getMetadata().getSchema().getClassByClusterId(new ORecordId(iTarget).clusterId);

      return getDatabase().getMetadata().getSchema().getClass(iTarget);
    }
    return null;
  }

  protected Object convertValue(OClass iClass, String fieldName, Object v) {
    if (iClass != null) {
      // CHECK TYPE AND CONVERT IF NEEDED
      final OProperty p = iClass.getProperty(fieldName);
      if (p != null) {
        final OClass embeddedType = p.getLinkedClass();

        switch (p.getType()) {
        case EMBEDDED:
          // CONVERT MAP IN DOCUMENTS ASSIGNING THE CLASS TAKEN FROM SCHEMA
          if (v instanceof Map)
            v = createDocumentFromMap(embeddedType, (Map<String, Object>) v);
          break;

        case EMBEDDEDSET:
          // CONVERT MAPS IN DOCUMENTS ASSIGNING THE CLASS TAKEN FROM SCHEMA
          if (v instanceof Map)
            return createDocumentFromMap(embeddedType, (Map<String, Object>) v);
          else if (OMultiValue.isMultiValue(v)) {
            final Set set = new HashSet();

            for (Object o : OMultiValue.getMultiValueIterable(v)) {
              if (o instanceof Map) {
                final ODocument doc = createDocumentFromMap(embeddedType, (Map<String, Object>) o);
                set.add(doc);
              } else if (o instanceof OIdentifiable)
                set.add(((OIdentifiable) o).getRecord());
              else
                set.add(o);
            }

            v = set;
          }
          break;

        case EMBEDDEDLIST:
          // CONVERT MAPS IN DOCUMENTS ASSIGNING THE CLASS TAKEN FROM SCHEMA
          if (v instanceof Map)
            return createDocumentFromMap(embeddedType, (Map<String, Object>) v);
          else if (OMultiValue.isMultiValue(v)) {
            final List set = new ArrayList();

            for (Object o : OMultiValue.getMultiValueIterable(v)) {
              if (o instanceof Map) {
                final ODocument doc = createDocumentFromMap(embeddedType, (Map<String, Object>) o);
                set.add(doc);
              } else if (o instanceof OIdentifiable)
                set.add(((OIdentifiable) o).getRecord());
              else
                set.add(o);
            }

            v = set;
          }
          break;

        case EMBEDDEDMAP:
          // CONVERT MAPS IN DOCUMENTS ASSIGNING THE CLASS TAKEN FROM SCHEMA
          if (v instanceof Map) {
            final Map<String, Object> map = new HashMap<String, Object>();

            for (Map.Entry<String, Object> entry : ((Map<String, Object>) v).entrySet()) {
              if (entry.getValue() instanceof Map) {
                final ODocument doc = createDocumentFromMap(embeddedType, (Map<String, Object>) entry.getValue());
                map.put(entry.getKey(), doc);
              } else if (entry.getValue() instanceof OIdentifiable)
                map.put(entry.getKey(), ((OIdentifiable) entry.getValue()).getRecord());
              else
                map.put(entry.getKey(), entry.getValue());
            }

            v = map;
          }
          break;
        }
      }
    }
    return v;
  }

  private ODocument createDocumentFromMap(OClass embeddedType, Map<String, Object> o) {
    final ODocument doc = new ODocument();
    if (embeddedType != null)
      doc.setClassName(embeddedType.getName());

    doc.fromMap(o);
    return doc;
  }

  @Override
  public long getDistributedTimeout() {
    return OGlobalConfiguration.DISTRIBUTED_COMMAND_TASK_SYNCH_TIMEOUT.getValueAsLong();
  }

  protected Object getFieldValueCountingParameters(String fieldValue) {
    if (fieldValue.trim().equals("?"))
      parameterCounter++;
    return OSQLHelper.parseValue(this, fieldValue, context);
  }

  protected ODocument parseJSON() {
    final String contentAsString = parserRequiredWord(false, "JSON expected").trim();
    final ODocument json = new ODocument().fromJSON(contentAsString);
    parserSkipWhiteSpaces();
    return json;
  }

}
