/*
 * Copyright 2014 Orient Technologies.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.orientechnologies.lucene.operator;

import com.orientechnologies.orient.core.sql.operator.OQueryOperator;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class OLuceneOperatorFactory implements OQueryOperatorFactory {

  public static final Set<OQueryOperator> OPERATORS;

  static {
    final Set<OQueryOperator> operators = new HashSet<OQueryOperator>();
    operators.add(new OLuceneNearOperator());
    operators.add(new OLuceneWithinOperator());
    operators.add(new OLuceneTextOperator());
    operators.add(new OLuceneExpTextOperator());
//    operators.add(new OLuceneSTContainsOperator());
//    operators.add(new OLuceneSTNearOperator());
//    operators.add(new OLuceneSTWithinOperator());
    operators.add(new OLuceneOverlapOperator());
    OPERATORS = Collections.unmodifiableSet(operators);
  }

  @Override
  public Set<OQueryOperator> getOperators() {
    return OPERATORS;
  }
}