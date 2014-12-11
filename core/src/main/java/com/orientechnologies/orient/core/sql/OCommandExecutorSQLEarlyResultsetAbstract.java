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

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.orientechnologies.orient.core.db.record.OIdentifiable;

/**
 * Abstract class that early executes the command and provide the iterator interface on top of the resultset.
 * 
 * @author Luca Garulli
 */
@SuppressWarnings("unchecked")
public abstract class OCommandExecutorSQLEarlyResultsetAbstract extends OCommandExecutorSQLResultsetAbstract {
  private Iterator<OIdentifiable> iterator;

  public Iterator<OIdentifiable> iterator() {
    return iterator(null);
  }

  @Override
  public Iterator<OIdentifiable> iterator(Map<Object, Object> iArgs) {
    if (iterator == null) {
      if (tempResult == null)
        tempResult = (List<OIdentifiable>) execute(iArgs);
      iterator = tempResult.iterator();
    }
    return iterator;
  }
}
