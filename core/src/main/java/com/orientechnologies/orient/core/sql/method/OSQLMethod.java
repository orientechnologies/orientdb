/*
 * Copyright 2013 Geomatys.
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
package com.orientechnologies.orient.core.sql.method;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;

/**
 * Methods can be used on various objects with different number of arguments. SQL syntax :
 * <object_name>.<method_name>([parameters])
 *
 * @author Johann Sorel (Geomatys)
 */
public interface OSQLMethod extends Comparable<OSQLMethod> {

  /** @return method name */
  String getName();

  /**
   * Returns a convinient SQL String representation of the method.
   *
   * <p>Example :
   *
   * <pre>
   *  field.myMethod( param1, param2, [optionalParam3])
   * </pre>
   *
   * This text will be used in exception messages.
   *
   * @return String , never null.
   */
  public String getSyntax();

  /** @return minimum number of arguments requiered by this method */
  int getMinParams();

  /** @return maximum number of arguments requiered by this method */
  int getMaxParams();

  /**
   * Process a record.
   *
   * @param iThis
   * @param iCurrentRecord : current record
   * @param iContext execution context
   * @param ioResult : field value
   * @param iParams : function parameters, number is ensured to be within minParams and maxParams.
   * @return evaluation result
   */
  Object execute(
      Object iThis,
      OIdentifiable iCurrentRecord,
      OCommandContext iContext,
      Object ioResult,
      Object[] iParams);

  boolean evaluateParameters();
}
