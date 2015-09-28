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
package com.orientechnologies.orient.server.network.protocol.http.command.post;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
 import com.orientechnologies.orient.core.id.ORID;
 import com.orientechnologies.orient.core.id.ORecordId;
 import com.orientechnologies.orient.core.record.impl.ODocument;
 import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
 import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
 import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
 import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandDocumentAbstract;

public class OServerCommandPostDocument extends OServerCommandDocumentAbstract {
   private static final String[] NAMES = { "POST|document/*" };

   @Override
   public boolean execute(final OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
     checkSyntax(iRequest.url, 2, "Syntax error: document/<database>");

     iRequest.data.commandInfo = "Create document";

     ODatabaseDocumentTx db = null;

     ODocument doc;

     try {
       db = getProfiledDatabaseInstance(iRequest);

       doc = new ODocument().fromJSON(iRequest.content);
       doc.getRecordVersion().reset();

       // ASSURE TO MAKE THE RECORD ID INVALID
       ((ORecordId) doc.getIdentity()).clusterPosition = ORID.CLUSTER_POS_INVALID;

       doc.save();

       iResponse.send(OHttpUtils.STATUS_CREATED_CODE, OHttpUtils.STATUS_CREATED_DESCRIPTION, OHttpUtils.CONTENT_JSON,
           doc.toJSON(), OHttpUtils.HEADER_ETAG + doc.getVersion());

     } finally {
       if (db != null)
         db.close();
     }
     return false;
   }

   @Override
   public String[] getNames() {
     return NAMES;
   }
 }
