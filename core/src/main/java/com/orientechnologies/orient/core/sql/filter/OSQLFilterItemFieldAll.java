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
package com.orientechnologies.orient.core.sql.filter;

import com.orientechnologies.common.parser.OBaseParser;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;

/**
 * Represent one or more object fields as value in the query condition.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OSQLFilterItemFieldAll extends OSQLFilterItemFieldMultiAbstract {
  public static final String NAME = "ALL";
  public static final String FULL_NAME = "ALL()";

  public OSQLFilterItemFieldAll(
      final OSQLPredicate iQueryCompiled, final String iName, final OClass iClass) {
    super(iQueryCompiled, iName, iClass, OStringSerializerHelper.getParameters(iName));
  }

  @Override
  public String getRoot() {
    return FULL_NAME;
  }

  @Override
  protected void setRoot(final OBaseParser iQueryToParse, final String iRoot) {}
}
