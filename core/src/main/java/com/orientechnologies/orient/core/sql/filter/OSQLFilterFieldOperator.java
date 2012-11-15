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

/**
 * Query operators relative a field or a set of fields.
 * 
 * @author Luca Garulli
 * 
 */
public enum OSQLFilterFieldOperator {
  FIELD(-2, "FIELD"), SIZE(0, "SIZE"), LENGTH(1, "LENGTH"), TOUPPERCASE(2, "TOUPPERCASE"), TOLOWERCASE(3, "TOLOWERCASE"), TRIM(4,
      "TRIM"), LEFT(5, "LEFT", 1), RIGHT(6, "RIGHT", 1), SUBSTRING(7, "SUBSTRING", 1, 2), CHARAT(8, "CHARAT", 1), INDEXOF(9,
      "INDEXOF", 1, 2), FORMAT(10, "FORMAT", 1), APPEND(11, "APPEND", 1), PREFIX(12, "PREFIX", 1), ASSTRING(13, "ASSTRING"), ASINTEGER(
      14, "ASINTEGER"), ASLONG(15, "ASLONG"), ASFLOAT(16, "ASFLOAT"), ASDATE(17, "ASDATE"), ASDATETIME(18, "ASDATETIME"), ASBOOLEAN(
      19, "ASBOOLEAN"), TOJSON(20, "TOJSON"), KEYS(21, "KEYS"), VALUES(22, "VALUES"), REPLACE(23, "REPLACE", 2);

  protected static final OSQLFilterFieldOperator[] OPERATORS       = { FIELD, SIZE, LENGTH, TOUPPERCASE, TOLOWERCASE, TRIM, LEFT,
      RIGHT, SUBSTRING, CHARAT, INDEXOF, FORMAT, APPEND, PREFIX, ASSTRING, ASINTEGER, ASLONG, ASFLOAT, ASDATE, ASDATETIME, ASBOOLEAN,
      TOJSON, KEYS, VALUES                                        };

  public static final String                       CHAIN_SEPARATOR = ".";

  public final int                                 id;
  public final String                              keyword;
  public final int                                 minArguments;
  public final int                                 maxArguments;

  OSQLFilterFieldOperator(final int iId, final String iKeyword) {
    this(iId, iKeyword, 0, 0);
  }

  OSQLFilterFieldOperator(final int iId, final String iKeyword, int iExactArgs) {
    id = iId;
    keyword = iKeyword;
    minArguments = iExactArgs;
    maxArguments = iExactArgs;
  }

  OSQLFilterFieldOperator(final int iId, final String iKeyword, int iMinArgs, int iMaxArgs) {
    id = iId;
    keyword = iKeyword;
    minArguments = iMinArgs;
    maxArguments = iMaxArgs;
  }

  public static OSQLFilterFieldOperator getById(final int iId) {
    for (OSQLFilterFieldOperator o : OPERATORS) {
      if (iId == o.id)
        return o;
    }
    return null;
  }

  @Override
  public String toString() {
    return keyword;
  }
}
