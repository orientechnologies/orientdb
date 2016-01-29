/*
 *
 *  * Copyright 2010-2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.orientechnologies.orient.etl.listener;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.record.impl.ODocument;

public interface OImporterListener {
  void onBeforeFile(ODatabaseDocumentTx db, OCommandContext iContext);

  void onAfterFile(ODatabaseDocumentTx db, OCommandContext iContext);

  boolean onBeforeLine(ODatabaseDocumentTx db, OCommandContext iContext);

  void onAfterLine(ODatabaseDocumentTx db, OCommandContext iContext);

  void onDump(ODatabaseDocumentTx db, OCommandContext iContext);

  void onJoinNotFound(ODatabaseDocumentTx db, OCommandContext iContext, final OIndex<?> iIndex, final Object iKey);

  void validate(ODatabaseDocumentTx db, OCommandContext iContext, ODocument iRecord);
}
