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
package com.orientechnologies.orient.server.network.protocol.http.command.get;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
 import com.orientechnologies.orient.core.serialization.serializer.OJSONWriter;
 import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
 import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
 import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
 import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedDbAbstract;

 import java.io.StringWriter;
 import java.util.Collections;
 import java.util.List;

public class OServerCommandGetStorageAllocation extends OServerCommandAuthenticatedDbAbstract {
   private static final String[] NAMES = { "GET|allocation/*" };

   @Override
   public boolean execute(final OHttpRequest iRequest, final OHttpResponse iResponse) throws Exception {
     String[] urlParts = checkSyntax(iRequest.url, 2, "Syntax error: allocation/<database>");

     iRequest.data.commandInfo = "Storage allocation";
     iRequest.data.commandDetail = urlParts[1];

     throw new IllegalArgumentException("Cannot get allocation information for database '" + iRequest.databaseName
         + "' because it is not implemented yet.");
   }

   @Override
   public String[] getNames() {
     return NAMES;
   }
 }
