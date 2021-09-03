/**
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * <p>For more information: http://www.orientdb.com
 */
package com.orientechnologies.spatial.operator;

import com.orientechnologies.orient.core.sql.operator.OQueryOperator;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorFactory;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class OLuceneSpatialOperatorFactory implements OQueryOperatorFactory {

  public static final Set<OQueryOperator> OPERATORS;

  static {
    final Set<OQueryOperator> operators =
        new HashSet<OQueryOperator>() {
          {
            add(new OLuceneNearOperator());
            add(new OLuceneWithinOperator());
            add(new OLuceneOverlapOperator());
          }
        };

    OPERATORS = Collections.unmodifiableSet(operators);
  }

  @Override
  public Set<OQueryOperator> getOperators() {
    return OPERATORS;
  }
}
