/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.core.sql;

import com.orientechnologies.common.util.OPatternConst;
import com.orientechnologies.orient.core.collate.OCollate;
import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexDefinitionFactory;
import com.orientechnologies.orient.core.index.OIndexFactory;
import com.orientechnologies.orient.core.index.OIndexes;
import com.orientechnologies.orient.core.index.OPropertyMapIndexDefinition;
import com.orientechnologies.orient.core.index.ORuntimeKeyIndexDefinition;
import com.orientechnologies.orient.core.index.OSimpleKeyIndexDefinition;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClassImpl;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * SQL CREATE INDEX command: Create a new index against a property.
 * <p/>
 * <p>
 * Supports following grammar: <br>
 * "CREATE" "INDEX" &lt;indexName&gt; ["ON" &lt;className&gt; "(" &lt;propName&gt; ("," &lt;propName&gt;)* ")"] &lt;indexType&gt;
 * [&lt;keyType&gt; ("," &lt;keyType&gt;)*]
 * </p>
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
@SuppressWarnings("unchecked")
public class OCommandExecutorSQLCreateIndex extends OCommandExecutorSQLAbstract implements OCommandDistributedReplicateRequest {
  public static final String KEYWORD_CREATE   = "CREATE";
  public static final String KEYWORD_INDEX    = "INDEX";
  public static final String KEYWORD_ON       = "ON";
  public static final String KEYWORD_METADATA = "METADATA";
  public static final String KEYWORD_ENGINE   = "ENGINE";

  private String             indexName;
  private OClass             oClass;
  private String[]           fields;
  private OClass.INDEX_TYPE  indexType;
  private OType[]            keyTypes;
  private byte               serializerKeyId;
  private String             engine;
  private ODocument          metadataDoc      = null;
  private String[]           collates;

  public OCommandExecutorSQLCreateIndex parse(final OCommandRequest iRequest) {
    init((OCommandRequestText) iRequest);

    final StringBuilder word = new StringBuilder();

    int oldPos = 0;
    int pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);
    if (pos == -1 || !word.toString().equals(KEYWORD_CREATE))
      throw new OCommandSQLParsingException("Keyword " + KEYWORD_CREATE + " not found. Use " + getSyntax(), parserText, oldPos);

    oldPos = pos;
    pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);
    if (pos == -1 || !word.toString().equals(KEYWORD_INDEX))
      throw new OCommandSQLParsingException("Keyword " + KEYWORD_INDEX + " not found. Use " + getSyntax(), parserText, oldPos);

    oldPos = pos;
    pos = nextWord(parserText, parserTextUpperCase, oldPos, word, false);
    if (pos == -1)
      throw new OCommandSQLParsingException("Expected index name. Use " + getSyntax(), parserText, oldPos);

    indexName = word.toString();

    oldPos = pos;
    pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);
    if (pos == -1)
      throw new OCommandSQLParsingException("Index type requested. Use " + getSyntax(), parserText, oldPos + 1);

    if (word.toString().equals(KEYWORD_ON)) {
      oldPos = pos;
      pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);
      if (pos == -1)
        throw new OCommandSQLParsingException("Expected class name. Use " + getSyntax(), parserText, oldPos);
      oldPos = pos;
      oClass = findClass(word.toString());

      if (oClass == null)
        throw new OCommandExecutionException("Class " + word + " not found");

      pos = parserTextUpperCase.indexOf(")");
      if (pos == -1) {
        throw new OCommandSQLParsingException("No right bracket found. Use " + getSyntax(), parserText, oldPos);
      }

      final String props = parserText.substring(oldPos, pos).trim().substring(1);

      List<String> propList = new ArrayList<String>();
      Collections.addAll(propList, OPatternConst.PATTERN_COMMA_SEPARATED.split(props.trim()));

      fields = new String[propList.size()];
      propList.toArray(fields);

      for (int i = 0; i < fields.length; i++) {
        final String fieldName = fields[i];

        final int collatePos = fieldName.toUpperCase().indexOf(" COLLATE ");

        if (collatePos > 0) {
          if (collates == null)
            collates = new String[fields.length];

          collates[i] = fieldName.substring(collatePos + " COLLATE ".length()).toLowerCase().trim();
          fields[i] = fieldName.substring(0, collatePos);
        } else {
          if (collates != null)
            collates[i] = null;
        }
      }

      for (String propToIndex : fields) {
        checkMapIndexSpecifier(propToIndex, parserText, oldPos);

        propList.add(propToIndex);
      }

      oldPos = pos + 1;
      pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);
      if (pos == -1)
        throw new OCommandSQLParsingException("Index type requested. Use " + getSyntax(), parserText, oldPos + 1);
    } else {
      if (indexName.indexOf('.') > 0) {
        final String[] parts = indexName.split("\\.");

        oClass = findClass(parts[0]);
        if (oClass == null)
          throw new OCommandExecutionException("Class " + parts[0] + " not found");

        fields = new String[] { parts[1] };
      }
    }

    indexType = OClass.INDEX_TYPE.valueOf(word.toString());

    if (indexType == null)
      throw new OCommandSQLParsingException("Index type is null", parserText, oldPos);

    oldPos = pos;
    pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);

    if (word.toString().equals(KEYWORD_ENGINE)) {
      oldPos = pos;
      pos = nextWord(parserText, parserTextUpperCase, oldPos, word, false);
      oldPos = pos;
      engine = word.toString().toUpperCase();
    } else
      parserGoBack();

    final int configPos = parserTextUpperCase.indexOf(KEYWORD_METADATA, oldPos);

    if (configPos > -1) {
      final String configString = parserText.substring(configPos + KEYWORD_METADATA.length()).trim();
      metadataDoc = new ODocument().fromJSON(configString);
    }

    pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);
    if (pos != -1 && !word.toString().equalsIgnoreCase("NULL") && !word.toString().equalsIgnoreCase(KEYWORD_METADATA)) {
      final String typesString;
      if (configPos > -1)
        typesString = parserTextUpperCase.substring(oldPos, configPos).trim();
      else
        typesString = parserTextUpperCase.substring(oldPos).trim();

      if (word.toString().equalsIgnoreCase("RUNTIME")) {
        oldPos = pos;
        pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);

        serializerKeyId = Byte.parseByte(word.toString());
      } else {
        ArrayList<OType> keyTypeList = new ArrayList<OType>();
        for (String typeName : OPatternConst.PATTERN_COMMA_SEPARATED.split(typesString)) {
          keyTypeList.add(OType.valueOf(typeName));
        }

        keyTypes = new OType[keyTypeList.size()];
        keyTypeList.toArray(keyTypes);

        if (fields != null && fields.length != 0 && fields.length != keyTypes.length) {
          throw new OCommandSQLParsingException("Count of fields does not match with count of property types. " + "Fields: "
              + Arrays.toString(fields) + "; Types: " + Arrays.toString(keyTypes), parserText, oldPos);
        }
      }
    }

    return this;
  }

  /**
   * Execute the CREATE INDEX.
   */
  @SuppressWarnings("rawtypes")
  public Object execute(final Map<Object, Object> iArgs) {
    if (indexName == null)
      throw new OCommandExecutionException("Cannot execute the command because it has not been parsed yet");

    final ODatabaseDocument database = getDatabase();
    final OIndex<?> idx;
    List<OCollate> collatesList = null;

    if (collates != null) {
      collatesList = new ArrayList<OCollate>();

      for (String collate : collates) {
        if (collate != null) {
          final OCollate col = OSQLEngine.getCollate(collate);
          collatesList.add(col);
        } else
          collatesList.add(null);
      }
    }

    if (fields == null || fields.length == 0) {
      OIndexFactory factory = OIndexes.getFactory(indexType.toString(), null);

      if (keyTypes != null)
        idx = database
            .getMetadata()
            .getIndexManager()
            .createIndex(indexName, indexType.toString(),
                new OSimpleKeyIndexDefinition(keyTypes, collatesList, factory.getLastVersion()), null, null, metadataDoc, engine);
      else if (serializerKeyId != 0) {
        idx = database
            .getMetadata()
            .getIndexManager()
            .createIndex(indexName, indexType.toString(),
                new ORuntimeKeyIndexDefinition(serializerKeyId, factory.getLastVersion()), null, null, metadataDoc, engine);
      } else
        idx = database.getMetadata().getIndexManager()
            .createIndex(indexName, indexType.toString(), null, null, null, metadataDoc, engine);
    } else {
      if ((keyTypes == null || keyTypes.length == 0) && collates == null) {
        idx = oClass.createIndex(indexName, indexType.toString(), null, metadataDoc, engine, fields);
      } else {
        final List<OType> fieldTypeList = keyTypes != null ? Arrays.asList(keyTypes) : ((OClassImpl) oClass)
            .extractFieldTypes(fields);

        final OIndexDefinition idxDef = OIndexDefinitionFactory.createIndexDefinition(oClass, Arrays.asList(fields), fieldTypeList,
            collatesList, indexType.toString(), null);

        idx = database.getMetadata().getIndexManager()
            .createIndex(indexName, indexType.name(), idxDef, oClass.getPolymorphicClusterIds(), null, metadataDoc, engine);
      }
    }

    if (idx != null)
      return idx.getSize();

    return null;
  }

  @Override
  public QUORUM_TYPE getQuorumType() {
    return QUORUM_TYPE.ALL;
  }

  @Override
  public String getSyntax() {
    return "CREATE INDEX <name> [ON <class-name> (prop-names [COLLATE <collate>])] <type> [<key-type>] [METADATA {JSON Index Metadata Document}]";
  }

  private OClass findClass(String part) {
    return getDatabase().getMetadata().getSchema().getClass(part);
  }

  private void checkMapIndexSpecifier(final String fieldName, final String text, final int pos) {
    final String[] fieldNameParts = OPatternConst.PATTERN_SPACES.split(fieldName);
    if (fieldNameParts.length == 1)
      return;

    if (fieldNameParts.length == 3) {
      if ("by".equals(fieldNameParts[1].toLowerCase())) {
        try {
          OPropertyMapIndexDefinition.INDEX_BY.valueOf(fieldNameParts[2].toUpperCase());
        } catch (IllegalArgumentException iae) {
          throw new OCommandSQLParsingException("Illegal field name format, should be '<property> [by key|value]' but was '"
              + fieldName + "'", text, pos, iae);
        }
        return;
      }
      throw new OCommandSQLParsingException("Illegal field name format, should be '<property> [by key|value]' but was '"
          + fieldName + "'", text, pos);
    }

    throw new OCommandSQLParsingException("Illegal field name format, should be '<property> [by key|value]' but was '" + fieldName
        + "'", text, pos);
  }
}
