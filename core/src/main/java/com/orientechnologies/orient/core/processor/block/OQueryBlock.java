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
package com.orientechnologies.orient.core.processor.block;

import java.util.List;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OTransactionException;
import com.orientechnologies.orient.core.processor.OComposableProcessor;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

public class OQueryBlock extends OAbstractBlock {
  @Override
  public Object processBlock(final OComposableProcessor iManager, final OCommandContext iContext, final ODocument iConfig,
      ODocument iOutput, final boolean iReadOnly) {
    if (!(iConfig instanceof ODocument))
      throw new OTransactionException("QueryBlock: expected document as content");

    String command = parse(iContext, (ODocument) iConfig);

    command = (String) resolveValue(iContext, command);

    debug(iContext, "Executing: " + (iReadOnly ? "query" : "command") + ": " + command + "...");

    final OCommandRequestText cmd = new OSQLSynchQuery<OIdentifiable>(command.toString());

    cmd.getContext().setParent(iContext);

    // CREATE THE RIGHT COMMAND BASED ON IDEMPOTENCY
    // final OCommandRequestText cmd = iReadOnly ? new OSQLSynchQuery<OIdentifiable>(command.toString()) : new OCommandSQL(
    // command.toString());

    final String returnVariable = getFieldOfClass(iContext, iConfig, "return", String.class);
    if (returnVariable != null) {
      final List<?> result = cmd.execute();
      debug(iContext, "Returned %d records", result.size());
      return result;
    }

    return cmd;
  }

  @Override
  public String getName() {
    return "query";
  }

  protected String parse(final OCommandContext iContext, final ODocument iContent) {
    final Object code = getField(iContext, iContent, "code");
    if (code != null)
      // CODE MODE
      return code.toString();

    // SINGLE FIELDS MODE
    final StringBuilder command = new StringBuilder();
    command.append("select ");

    generateProjections(iContext, iContent, command);
    generateTarget(iContext, iContent, command);
    generateLet(iContext, iContent, command);
    generateLimit(iContext, iContent, command);
    generateFilter(iContext, iContent, command);
    generateGroupBy(iContext, iContent, command);
    generateOrderBy(iContext, iContent, command);
    generateLimit(iContext, iContent, command);

    return command.toString();
  }

  @SuppressWarnings("unchecked")
  private void generateProjections(final OCommandContext iContext, ODocument iInput, final StringBuilder iCommandText) {
    final Object fields = getField(iContext, iInput, "fields");

    if (fields instanceof String)
      iCommandText.append(fields.toString());
    else {
      final List<ODocument> fieldList = (List<ODocument>) fields;
      if (fieldList != null) {
        boolean first = true;
        for (Object field : fieldList) {
          if (first)
            first = false;
          else
            iCommandText.append(", ");

          if (field instanceof ODocument) {
            final ODocument fieldDoc = (ODocument) field;
            for (String f : fieldDoc.fieldNames()) {
              iCommandText.append(f);
              iCommandText.append(" as ");
              iCommandText.append(fieldDoc.field(f));
            }
          } else
            iCommandText.append(field.toString());
        }
      }
    }
  }

  private void generateTarget(final OCommandContext iContext, ODocument iInput, final StringBuilder iCommandText) {
    final String target = getField(iContext, iInput, "target");
    if (target != null) {
      iCommandText.append(" from ");
      iCommandText.append(target);
    }
  }

  private void generateLet(final OCommandContext iContext, ODocument iInput, final StringBuilder iCommandText) {
    final String let = getField(iContext, iInput, "let");
    if (let != null) {
      iCommandText.append(" let ");
      iCommandText.append(let);
    }
  }

  private void generateFilter(final OCommandContext iContext, ODocument iInput, final StringBuilder iCommandText) {
    final String filter = getField(iContext, iInput, "filter");
    if (filter != null) {
      iCommandText.append(" where ");
      iCommandText.append(filter);
    }
  }

  private void generateGroupBy(final OCommandContext iContext, ODocument iInput, final StringBuilder iCommandText) {
    final String groupBy = getField(iContext, iInput, "groupBy");
    if (groupBy != null) {
      iCommandText.append(" group by ");
      iCommandText.append(groupBy);
    }
  }

  private void generateOrderBy(final OCommandContext iContext, ODocument iInput, final StringBuilder iCommandText) {
    final String orderBy = getField(iContext, iInput, "orderBy");
    if (orderBy != null) {
      iCommandText.append(" order by ");
      iCommandText.append(orderBy);
    }
  }

  private void generateLimit(final OCommandContext iContext, ODocument iInput, final StringBuilder iCommandText) {
    final Integer limit = getField(iContext, iInput, "limit");
    if (limit != null) {
      iCommandText.append(" limit ");
      iCommandText.append(limit);
    }
  }
}