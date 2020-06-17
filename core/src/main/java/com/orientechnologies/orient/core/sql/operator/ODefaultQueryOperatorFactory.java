/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
package com.orientechnologies.orient.core.sql.operator;

import com.orientechnologies.orient.core.sql.operator.math.OQueryOperatorDivide;
import com.orientechnologies.orient.core.sql.operator.math.OQueryOperatorMinus;
import com.orientechnologies.orient.core.sql.operator.math.OQueryOperatorMod;
import com.orientechnologies.orient.core.sql.operator.math.OQueryOperatorMultiply;
import com.orientechnologies.orient.core.sql.operator.math.OQueryOperatorPlus;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Default operator factory.
 *
 * @author Johann Sorel (Geomatys)
 */
public class ODefaultQueryOperatorFactory implements OQueryOperatorFactory {

  private static final Set<OQueryOperator> OPERATORS;

  static {
    final Set<OQueryOperator> operators = new HashSet<OQueryOperator>();
    operators.add(new OQueryOperatorEquals());
    operators.add(new OQueryOperatorAnd());
    operators.add(new OQueryOperatorOr());
    operators.add(new OQueryOperatorNotEquals());
    operators.add(new OQueryOperatorNotEquals2());
    operators.add(new OQueryOperatorNot());
    operators.add(new OQueryOperatorMinorEquals());
    operators.add(new OQueryOperatorMinor());
    operators.add(new OQueryOperatorMajorEquals());
    operators.add(new OQueryOperatorContainsAll());
    operators.add(new OQueryOperatorMajor());
    operators.add(new OQueryOperatorLike());
    operators.add(new OQueryOperatorMatches());
    operators.add(new OQueryOperatorInstanceof());
    operators.add(new OQueryOperatorIs());
    operators.add(new OQueryOperatorIn());
    operators.add(new OQueryOperatorContainsKey());
    operators.add(new OQueryOperatorContainsValue());
    operators.add(new OQueryOperatorContainsText());
    operators.add(new OQueryOperatorContains());
    operators.add(new OQueryOperatorTraverse());
    operators.add(new OQueryOperatorBetween());
    operators.add(new OQueryOperatorPlus());
    operators.add(new OQueryOperatorMinus());
    operators.add(new OQueryOperatorMultiply());
    operators.add(new OQueryOperatorDivide());
    operators.add(new OQueryOperatorMod());
    OPERATORS = Collections.unmodifiableSet(operators);
  }

  /** {@inheritDoc} */
  public Set<OQueryOperator> getOperators() {
    return OPERATORS;
  }
}
