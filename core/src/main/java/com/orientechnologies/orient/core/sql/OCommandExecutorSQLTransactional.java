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
package com.orientechnologies.orient.core.sql;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import java.util.Map;

/**
 * Acts as a delegate to the real command inserting the execution of the command inside a new
 * transaction if not yet begun.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OCommandExecutorSQLTransactional extends OCommandExecutorSQLDelegate {
  public static final String KEYWORD_TRANSACTIONAL = "TRANSACTIONAL";

  @SuppressWarnings("unchecked")
  @Override
  public OCommandExecutorSQLTransactional parse(OCommandRequest iCommand) {
    String cmd = ((OCommandSQL) iCommand).getText();
    super.parse(new OCommandSQL(cmd.substring(KEYWORD_TRANSACTIONAL.length())));
    return this;
  }

  @Override
  public Object execute(Map<Object, Object> iArgs) {
    final ODatabaseDocument database = getDatabase();
    boolean txbegun = database.getTransaction() == null || !database.getTransaction().isActive();

    if (txbegun) database.begin();

    try {
      final Object result = super.execute(iArgs);

      if (txbegun) database.commit();

      return result;
    } catch (Exception e) {
      if (txbegun) database.rollback();
      throw OException.wrapException(
          new OCommandExecutionException("Transactional command failed"), e);
    }
  }
}
