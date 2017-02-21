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

import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;

import java.util.Map;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 3/21/14
 */
public class OCommandExecutorSQLHide extends OCommandExecutorSQLAbstract {
  public static final String NAME         = "HIDE FROM";
  public static final String KEYWORD_HIDE = "HIDE";

  private ORID               recordIdToHide;

  public String getSyntax() {
    return "HIDE FROM RID";
  }

  @Override
  public OCommandExecutorSQLHide parse(OCommandRequest iRequest) {
    init((OCommandRequestText) iRequest);

    parserRequiredKeyword(OCommandExecutorSQLHide.KEYWORD_HIDE);
    parserRequiredKeyword(OCommandExecutorSQLHide.KEYWORD_FROM);

    final String subjectName = parserRequiredWord(false, "Syntax error", " =><,\r\n");
    recordIdToHide = new ORecordId(subjectName);

    return this;
  }

  @Override
  public long getDistributedTimeout() {
    return getDatabase().getConfiguration().getValueAsLong(OGlobalConfiguration.DISTRIBUTED_COMMAND_QUICK_TASK_SYNCH_TIMEOUT);
  }

  @Override
  public Object execute(Map<Object, Object> iArgs) {
    final ODatabaseDocumentInternal database = getDatabase();
    if (database.hide(recordIdToHide))
      return 1;

    return 0;
  }
}
