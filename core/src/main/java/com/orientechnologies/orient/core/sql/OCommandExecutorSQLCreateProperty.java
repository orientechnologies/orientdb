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

import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClassImpl;
import com.orientechnologies.orient.core.metadata.schema.OPropertyImpl;
import com.orientechnologies.orient.core.metadata.schema.OType;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * SQL CREATE PROPERTY command: Creates a new property in the target class.
 * 
 * @author Luca Garulli
 * @author Michael MacFadden
 */
@SuppressWarnings("unchecked")
public class OCommandExecutorSQLCreateProperty extends OCommandExecutorSQLAbstract implements OCommandDistributedReplicateRequest {
  public static final String KEYWORD_CREATE     = "CREATE";
  public static final String KEYWORD_PROPERTY   = "PROPERTY";
  public static final String KEYWORD_MANDATORY  = "MANDATORY";
  public static final String KEYWORD_NOTNULL    = "NOTNULL";
  public static final String KEYWORD_READONLY   = "READONLY";

  private String             className;
  private String             fieldName;
  private OType              type;
  private String             linked;
  private boolean            unsafe             = false;
  private boolean            mandatory          = false;
  private boolean            readOnly           = false;
  private boolean            notNull            = false;

  public OCommandExecutorSQLCreateProperty parse(final OCommandRequest iRequest) {
    init((OCommandRequestText) iRequest);

    final StringBuilder word = new StringBuilder();

    int oldPos = 0;
    int pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);
    if (pos == -1 || !word.toString().equals(KEYWORD_CREATE))
      throw new OCommandSQLParsingException("Keyword " + KEYWORD_CREATE + " not found", parserText, oldPos);

    oldPos = pos;
    pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);
    if (pos == -1 || !word.toString().equals(KEYWORD_PROPERTY))
      throw new OCommandSQLParsingException("Keyword " + KEYWORD_PROPERTY + " not found", parserText, oldPos);

    oldPos = pos;
    pos = nextWord(parserText, parserTextUpperCase, oldPos, word, false);
    if (pos == -1)
      throw new OCommandSQLParsingException("Expected <class>.<property>", parserText, oldPos);

    String[] parts = split(word);
    if (parts.length != 2)
      throw new OCommandSQLParsingException("Expected <class>.<property>", parserText, oldPos);

    className = parts[0];
    if (className == null)
      throw new OCommandSQLParsingException("Class not found", parserText, oldPos);
    fieldName = parts[1];

    oldPos = pos;
    pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);
    if (pos == -1)
      throw new OCommandSQLParsingException("Missed property type", parserText, oldPos);

    type = OType.valueOf(word.toString());

    oldPos = pos;
    pos = nextWord(parserText, parserTextUpperCase, oldPos, word, false);
    if (pos == -1)
    	return this;
    
    // We have another parsed word.  If it follows the type, then it could
    // be a linked or embedded type.  If so, it may be followed by the
    // linked or embedded type.
    switch(type) {
    case EMBEDDED:
    case EMBEDDEDMAP:
    case EMBEDDEDLIST:
    case EMBEDDEDSET:
    case LINK:
    case LINKMAP:
    case LINKLIST:
    case LINKSET:
    	// See if the parsed word is a keyword if it is not, then we assume it to 
    	// be the linked type/class.
    	// TODO handle escaped strings.
    	if (!isKeyword(word.toString())) {
    		// grab the word and look for the next
    		linked = word.toString();
    		oldPos = pos;
    	    pos = nextWord(parserText, parserTextUpperCase, oldPos, word, false);
    	    if (pos == -1)
    	    	return this;
    	} else {
    		// This was a keyword.  We need to back up to handle it later.
    		pos = oldPos;
    	}
    	default:
    		// not a type that should have a linked type, so back up to handle
    		// potential keywords.
    		pos = oldPos;
    }
    
    while(pos >= 0) {
    	oldPos = pos;
    	pos = parseKeyword(pos);
    }

    return this;
  }
  
  private int parseKeyword(int pos) {
	  final StringBuilder word = new StringBuilder();
	  int oldPos = pos;
      pos = nextWord(parserText, parserTextUpperCase, oldPos, word, false);
      
      String keyword = word.toString();
      if (KEYWORD_MANDATORY.equals(keyword)) {
    	  mandatory = true;
      } else if (KEYWORD_READONLY.equals(keyword)) {
    	  readOnly = true;
      } else if (KEYWORD_NOTNULL.equals(keyword)) {
    	  notNull = true;
      } else if (KEYWORD_UNSAFE.equals(keyword)) {
    	  unsafe = true;
      }
      
      return pos;
  }
  
  private boolean isKeyword(String text) {
	  return KEYWORD_MANDATORY.equals(text) ||
			  KEYWORD_NOTNULL.equals(text) ||
			  KEYWORD_READONLY.equals(text) ||
			  KEYWORD_UNSAFE.equals(text);
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
    // return word.toString().split("\\.");
  }

  @Override
  public long getDistributedTimeout() {
    return OGlobalConfiguration.DISTRIBUTED_COMMAND_TASK_SYNCH_TIMEOUT.getValueAsLong();
  }

  /**
   * Execute the CREATE PROPERTY.
   */
  public Object execute(final Map<Object, Object> iArgs) {
    if (type == null)
      throw new OCommandExecutionException("Cannot execute the command because it has not been parsed yet");

    final ODatabaseDocument database = getDatabase();
    final OClassImpl sourceClass = (OClassImpl) database.getMetadata().getSchema().getClass(className);
    if (sourceClass == null)
      throw new OCommandExecutionException("Source class '" + className + "' not found");

    OPropertyImpl prop = (OPropertyImpl) sourceClass.getProperty(fieldName);
    if (prop != null)
      throw new OCommandExecutionException("Property '" + className + "." + fieldName
          + "' already exists. Remove it before to retry.");

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
    OPropertyImpl property = sourceClass.addPropertyInternal(fieldName, type, linkedType, linkedClass, !unsafe);
    
    // Set attributes.  Note unsafe is handled above.
    property.setMandatory(mandatory);
    property.setNotNull(notNull);
    property.setReadonly(readOnly);
    
    return sourceClass.properties().size();
  }

  @Override
  public QUORUM_TYPE getQuorumType() {
    return QUORUM_TYPE.ALL;
  }

  @Override
  public String getSyntax() {
    return "CREATE PROPERTY <class>.<property> <type> [<linked-type>|<linked-class>] [UNSAFE | MANDATORY | READONLY | NOTNULL]";
  }
}
