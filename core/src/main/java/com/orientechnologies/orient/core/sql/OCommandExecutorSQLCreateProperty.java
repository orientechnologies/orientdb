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
package com.orientechnologies.orient.core.sql;

import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClassEmbedded;
import com.orientechnologies.orient.core.metadata.schema.OPropertyImpl;
import com.orientechnologies.orient.core.metadata.schema.OType;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * SQL CREATE PROPERTY command: Creates a new property in the target class.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 * @author Michael MacFadden
 */
@SuppressWarnings("unchecked")
public class OCommandExecutorSQLCreateProperty extends OCommandExecutorSQLAbstract
    implements OCommandDistributedReplicateRequest {
  public static final String KEYWORD_CREATE = "CREATE";
  public static final String KEYWORD_PROPERTY = "PROPERTY";

  public static final String KEYWORD_MANDATORY = "MANDATORY";
  public static final String KEYWORD_READONLY = "READONLY";
  public static final String KEYWORD_NOTNULL = "NOTNULL";
  public static final String KEYWORD_MIN = "MIN";
  public static final String KEYWORD_MAX = "MAX";
  public static final String KEYWORD_DEFAULT = "DEFAULT";

  private String className;
  private String fieldName;

  private boolean ifNotExists = false;

  private OType type;
  private String linked;

  private boolean readonly = false;
  private boolean mandatory = false;
  private boolean notnull = false;

  private String max = null;
  private String min = null;
  private String defaultValue = null;

  private boolean unsafe = false;

  public OCommandExecutorSQLCreateProperty parse(final OCommandRequest iRequest) {

    final OCommandRequestText textRequest = (OCommandRequestText) iRequest;

    String queryText = textRequest.getText();
    String originalQuery = queryText;
    try {
      queryText = preParse(queryText, iRequest);
      textRequest.setText(queryText);

      init((OCommandRequestText) iRequest);

      final StringBuilder word = new StringBuilder();

      int oldPos = 0;
      int pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);
      if (pos == -1 || !word.toString().equals(KEYWORD_CREATE))
        throw new OCommandSQLParsingException(
            "Keyword " + KEYWORD_CREATE + " not found", parserText, oldPos);

      oldPos = pos;
      pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);
      if (pos == -1 || !word.toString().equals(KEYWORD_PROPERTY))
        throw new OCommandSQLParsingException(
            "Keyword " + KEYWORD_PROPERTY + " not found", parserText, oldPos);

      oldPos = pos;
      pos = nextWord(parserText, parserTextUpperCase, oldPos, word, false);
      if (pos == -1)
        throw new OCommandSQLParsingException("Expected <class>.<property>", parserText, oldPos);

      String[] parts = split(word);
      if (parts.length != 2)
        throw new OCommandSQLParsingException("Expected <class>.<property>", parserText, oldPos);

      className = decodeClassName(parts[0]);
      if (className == null)
        throw new OCommandSQLParsingException("Class not found", parserText, oldPos);
      fieldName = decodeClassName(parts[1]);

      oldPos = pos;
      pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);
      if (pos == -1)
        throw new OCommandSQLParsingException("Missed property type", parserText, oldPos);
      if ("IF".equalsIgnoreCase(word.toString())) {
        oldPos = pos;
        pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);
        if (pos == -1)
          throw new OCommandSQLParsingException("Missed property type", parserText, oldPos);
        if (!"NOT".equalsIgnoreCase(word.toString())) {
          throw new OCommandSQLParsingException("Expected NOT EXISTS after IF", parserText, oldPos);
        }
        oldPos = pos;
        pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);
        if (pos == -1)
          throw new OCommandSQLParsingException("Missed property type", parserText, oldPos);
        if (!"EXISTS".equalsIgnoreCase(word.toString())) {
          throw new OCommandSQLParsingException("Expected EXISTS after IF NOT", parserText, oldPos);
        }
        this.ifNotExists = true;

        oldPos = pos;
        pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);
      }

      type = OType.valueOf(word.toString());

      // Use a REGEX for the rest because we know exactly what we are looking for.
      // If we are in strict mode, the parser took care of strict matching.
      String rest = parserTextUpperCase.substring(pos).trim();
      String pattern = "(`[^`]*`|[^\\(]\\S*)?\\s*(\\(.*\\))?\\s*(UNSAFE)?";

      Pattern r = Pattern.compile(pattern);
      Matcher m = r.matcher(rest.toUpperCase(Locale.ENGLISH).trim());

      if (m.matches()) {
        // Linked Type / Class
        if (m.group(1) != null) {
          if (m.group(1).equalsIgnoreCase("UNSAFE")) {
            this.unsafe = true;
          } else {
            linked = m.group(1);
            if (linked.startsWith("`") && linked.endsWith("`") && linked.length() > 1) {
              linked = linked.substring(1, linked.length() - 1);
            }
          }
        }

        // Attributes
        if (m.group(2) != null) {
          String raw = m.group(2);
          String atts = raw.substring(1, raw.length() - 1);
          processAtts(atts);
        }

        // UNSAFE
        if (m.group(3) != null) {
          this.unsafe = true;
        }
      } else {
        // Syntax Error
      }
    } finally {
      textRequest.setText(originalQuery);
    }
    return this;
  }

  private void processAtts(String atts) {
    String[] split = atts.split(",");
    for (String attDef : split) {
      String[] parts = attDef.trim().split("\\s+");
      if (parts.length > 2) {
        onInvalidAttributeDefinition(attDef);
      }

      String att = parts[0].trim();
      if (att.equals(KEYWORD_MANDATORY)) {
        this.mandatory = getOptionalBoolean(parts);
      } else if (att.equals(KEYWORD_READONLY)) {
        this.readonly = getOptionalBoolean(parts);
      } else if (att.equals(KEYWORD_NOTNULL)) {
        this.notnull = getOptionalBoolean(parts);
      } else if (att.equals(KEYWORD_MIN)) {
        this.min = getRequiredValue(attDef, parts);
      } else if (att.equals(KEYWORD_MAX)) {
        this.max = getRequiredValue(attDef, parts);
      } else if (att.equals(KEYWORD_DEFAULT)) {
        this.defaultValue = getRequiredValue(attDef, parts);
      } else {
        onInvalidAttributeDefinition(attDef);
      }
    }
  }

  private void onInvalidAttributeDefinition(String attDef) {
    throw new OCommandSQLParsingException("Invalid attribute definition: '" + attDef + "'");
  }

  private boolean getOptionalBoolean(String[] parts) {
    if (parts.length < 2) {
      return true;
    }

    String trimmed = parts[1].trim();
    if (trimmed.length() == 0) {
      return true;
    }

    return Boolean.parseBoolean(trimmed);
  }

  private String getRequiredValue(String attDef, String[] parts) {
    if (parts.length < 2) {
      onInvalidAttributeDefinition(attDef);
    }

    String trimmed = parts[1].trim();
    if (trimmed.length() == 0) {
      onInvalidAttributeDefinition(attDef);
    }

    return trimmed;
  }

  private String[] split(StringBuilder word) {
    List<String> result = new ArrayList<String>();
    StringBuilder builder = new StringBuilder();
    boolean quoted = false;
    for (char c : word.toString().toCharArray()) {
      if (!quoted) {
        if (c == '`') {
          quoted = true;
        } else if (c == '.') {
          String nextToken = builder.toString().trim();
          if (nextToken.length() > 0) {
            result.add(nextToken);
          }
          builder = new StringBuilder();
        } else {
          builder.append(c);
        }
      } else {
        if (c == '`') {
          quoted = false;
        } else {
          builder.append(c);
        }
      }
    }
    String nextToken = builder.toString().trim();
    if (nextToken.length() > 0) {
      result.add(nextToken);
    }
    return result.toArray(new String[] {});
  }

  @Override
  public long getDistributedTimeout() {
    return getDatabase()
        .getConfiguration()
        .getValueAsLong(OGlobalConfiguration.DISTRIBUTED_COMMAND_QUICK_TASK_SYNCH_TIMEOUT);
  }

  /** Execute the CREATE PROPERTY. */
  public Object execute(final Map<Object, Object> iArgs) {
    if (type == null)
      throw new OCommandExecutionException(
          "Cannot execute the command because it has not been parsed yet");

    final ODatabaseDocument database = getDatabase();
    final OClassEmbedded sourceClass =
        (OClassEmbedded) database.getMetadata().getSchema().getClass(className);
    if (sourceClass == null)
      throw new OCommandExecutionException("Source class '" + className + "' not found");

    OPropertyImpl prop = (OPropertyImpl) sourceClass.getProperty(fieldName);

    if (prop != null) {
      if (ifNotExists) {
        return sourceClass.properties().size();
      }
      throw new OCommandExecutionException(
          "Property '"
              + className
              + "."
              + fieldName
              + "' already exists. Remove it before to retry.");
    }

    // CREATE THE PROPERTY
    OClass linkedClass = null;
    OType linkedType = null;
    if (linked != null) {
      // FIRST SEARCH BETWEEN CLASSES
      linkedClass = database.getMetadata().getSchema().getClass(linked);

      if (linkedClass == null)
        // NOT FOUND: SEARCH BETWEEN TYPES
        linkedType = OType.valueOf(linked.toUpperCase(Locale.ENGLISH));
    }

    // CREATE IT LOCALLY
    OPropertyImpl internalProp =
        sourceClass.addPropertyInternal(fieldName, type, linkedType, linkedClass, unsafe);
    if (readonly) {
      internalProp.setReadonly(true);
    }

    if (mandatory) {
      internalProp.setMandatory(true);
    }

    if (notnull) {
      internalProp.setNotNull(true);
    }

    if (max != null) {
      internalProp.setMax(max);
    }

    if (min != null) {
      internalProp.setMin(min);
    }

    if (defaultValue != null) {
      internalProp.setDefaultValue(defaultValue);
    }

    return sourceClass.properties().size();
  }

  @Override
  public QUORUM_TYPE getQuorumType() {
    return QUORUM_TYPE.ALL;
  }

  @Override
  public String getUndoCommand() {
    return "drop property " + className + "." + fieldName;
  }

  @Override
  public String getSyntax() {
    return "CREATE PROPERTY <class>.<property> [IF NOT EXISTS] <type> [<linked-type>|<linked-class>] "
        + "[(mandatory <true|false>, notnull <true|false>, <true|false>, default <value>, min <value>, max <value>)] "
        + "[UNSAFE]";
  }
}
