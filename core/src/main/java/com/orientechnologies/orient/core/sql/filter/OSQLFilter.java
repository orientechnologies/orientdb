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
package com.orientechnologies.orient.core.sql.filter;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.command.OCommandPredicate;
import com.orientechnologies.orient.core.exception.OQueryParsingException;
import com.orientechnologies.orient.core.record.ORecord;

/**
 * Parsed query. It's built once a query is parsed.
 * 
 * @author Luca Garulli
 * 
 */
public class OSQLFilter extends OSQLPredicate implements OCommandPredicate {
  public OSQLFilter(final String iText, final OCommandContext iContext, final String iFilterKeyword) {
    super();
    context = iContext;
    parserText = iText;
    parserTextUpperCase = iText.toUpperCase();

    try {
      final int lastPos = parserGetCurrentPosition();
      final String lastText = parserText;
      final String lastTextUpperCase = parserTextUpperCase;

      text(parserText.substring(lastPos));

      parserText = lastText;
      parserTextUpperCase = lastTextUpperCase;
      parserMoveCurrentPosition(lastPos);

    } catch (OQueryParsingException e) {
      if (e.getText() == null)
        // QUERY EXCEPTION BUT WITHOUT TEXT: NEST IT
        throw new OQueryParsingException("Error on parsing query", parserText, parserGetCurrentPosition(), e);

      throw e;
    } catch (Throwable t) {
      throw new OQueryParsingException("Error on parsing query", parserText, parserGetCurrentPosition(), t);
    }
  }

  public Object evaluate(final ORecord<?> iRecord, final OCommandContext iContext) {
    if (rootCondition == null)
      return true;

    return rootCondition.evaluate(iRecord, iContext);
  }

  public OSQLFilterCondition getRootCondition() {
    return rootCondition;
  }

  @Override
  public String toString() {
    if (rootCondition != null)
      return "Parsed: " + rootCondition.toString();
    return "Unparsed: " + parserText;
  }
}
