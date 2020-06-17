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
package com.orientechnologies.orient.core.sql.operator;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterCondition;
import java.util.regex.Pattern;

/**
 * MATCHES operator. Matches the left value against the regular expression contained in the second
 * one.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OQueryOperatorMatches extends OQueryOperatorEqualityNotNulls {

  public OQueryOperatorMatches() {
    super("MATCHES", 5, false);
  }

  @Override
  protected boolean evaluateExpression(
      final OIdentifiable iRecord,
      final OSQLFilterCondition iCondition,
      final Object iLeft,
      final Object iRight,
      OCommandContext iContext) {
    return this.matches(iLeft.toString(), (String) iRight, iContext);
  }

  @Override
  public OIndexReuseType getIndexReuseType(final Object iLeft, final Object iRight) {
    return OIndexReuseType.NO_INDEX;
  }

  @Override
  public ORID getBeginRidRange(final Object iLeft, final Object iRight) {
    return null;
  }

  @Override
  public ORID getEndRidRange(final Object iLeft, final Object iRight) {
    return null;
  }

  private boolean matches(
      final String iValue, final String iRegex, final OCommandContext iContext) {
    final String key = "MATCHES_" + iRegex.hashCode();
    Pattern p = (Pattern) iContext.getVariable(key);
    if (p == null) {
      p = Pattern.compile(iRegex);
      iContext.setVariable(key, p);
    }
    return p.matcher(iValue).matches();
  }
}
