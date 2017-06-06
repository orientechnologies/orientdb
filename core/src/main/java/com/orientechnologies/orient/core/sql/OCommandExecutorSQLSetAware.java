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
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.tx.OTransaction;

import java.util.*;

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

    boolean firstLap = true;
    while (!parserIsEnded() && (firstLap || parserGetLastSeparator() == ',' || parserGetCurrentChar() == ',')) {
      fieldName = parserRequiredWord(false, "Field name expected");
      if (fieldName.equalsIgnoreCase(KEYWORD_WHERE)) {
        parserGoBack();
        break;
      }

      parserNextChars(false, true, "=");
      fieldValue = parserRequiredWord(false, "Value expected", " =><,\r\n", true);

      // INSERT TRANSFORMED FIELD VALUE
      Object v = convertValue(iClass, fieldName, getFieldValueCountingParameters(fieldValue));
      v = reattachInTx(v);
      fields.add(new OPair(fieldName, v));
      parserSkipWhiteSpaces();
      firstLap = false;
    }

    if (fields.size() == 0)
      throwParsingException("Entries to set <field> = <value> are missed. Example: name = 'Bill', salary = 300.2");
  }

  protected Object reattachInTx(Object fVal) {
    if (fVal == null) {
      return null;
    }
    OTransaction tx = getDatabase().getTransaction();
    if (!tx.isActive()) {
      return fVal;
    }
    if (fVal instanceof ORID && ((ORID) fVal).isTemporary()) {
      ORecord txVal = tx.getRecord((ORID) fVal);
      if (txVal != null) {
        return txVal;
      }
    } else if (!(fVal instanceof OIdentifiable) && OMultiValue.isMultiValue(fVal)) {
      Iterator<Object> iter = OMultiValue.getMultiValueIterator(fVal);
      if (fVal instanceof List) {
        List<Object> result = new ArrayList<Object>();
        while (iter.hasNext()) {
          result.add(reattachInTx(iter.next()));
        }
        return result;
      } else if (fVal instanceof Set) {
        Set<Object> result = new HashSet<Object>();
        while (iter.hasNext()) {
          result.add(reattachInTx(iter.next()));
        }
        return result;
      }
    }
    return fVal;
  }


  protected OClass extractClassFromTarget(String iTarget) {
    // CLASS
    if (!iTarget.toUpperCase(Locale.ENGLISH).startsWith(OCommandExecutorSQLAbstract.CLUSTER_PREFIX)
        && !iTarget.startsWith(OCommandExecutorSQLAbstract.INDEX_PREFIX)) {

      if (iTarget.toUpperCase(Locale.ENGLISH).startsWith(OCommandExecutorSQLAbstract.CLASS_PREFIX))
        // REMOVE CLASS PREFIX
        iTarget = iTarget.substring(OCommandExecutorSQLAbstract.CLASS_PREFIX.length());

      if (iTarget.charAt(0) == ORID.PREFIX)
        return getDatabase().getMetadata().getSchema().getClassByClusterId(new ORecordId(iTarget).getClusterId());

      return getDatabase().getMetadata().getSchema().getClass(iTarget);
    }
    //CLUSTER
    if (iTarget.toUpperCase(Locale.ENGLISH).startsWith(OCommandExecutorSQLAbstract.CLUSTER_PREFIX)) {
      String clusterName = iTarget.substring(OCommandExecutorSQLAbstract.CLUSTER_PREFIX.length()).trim();
      ODatabaseDocumentInternal db = getDatabase();
      if(clusterName.startsWith("[") && clusterName.endsWith("]")) {
        String[] clusterNames = clusterName.substring(1, clusterName.length()-1).split(",");
        OClass candidateClass = null;
        for(String cName:clusterNames){
          OCluster aCluster = db.getStorage().getClusterByName(cName.trim());
          if(aCluster == null){
            return null;
          }
          OClass aClass = db.getMetadata().getSchema().getClassByClusterId(aCluster.getId());
          if(aClass == null){
            return null;
          }
          if(candidateClass == null || candidateClass.equals(aClass) || candidateClass.isSubClassOf(aClass)){
            candidateClass = aClass;
          }else if(!candidateClass.isSuperClassOf(aClass)){
            return null;
          }
        }
        return candidateClass;
      } else {
        OCluster cluster = db.getStorage().getClusterByName(clusterName);
        if (cluster != null) {
          return db.getMetadata().getSchema().getClassByClusterId(cluster.getId());
        }
      }
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
    return OSQLHelper.parseValue(this, fieldValue, context, true);
  }

  protected ODocument parseJSON() {
    final String contentAsString = parserRequiredWord(false, "JSON expected").trim();
    final ODocument json = new ODocument().fromJSON(contentAsString);
    parserSkipWhiteSpaces();
    return json;
  }

}
