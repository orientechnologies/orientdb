/*
 * Copyright 2015 OrientDB LTD (info(at)orientdb.com)
 *   
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *   
 *        http://www.apache.org/licenses/LICENSE-2.0
 *   
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *   
 *   For more information: http://www.orientdb.com
 */

package com.orientechnologies.agent.http.command;

import com.orientechnologies.orient.core.cache.OCommandCache;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;

import java.io.IOException;

/**
 * Created by Enrico Risa on 21/11/15.
 */
public class OServerCommandQueryCacheManager extends OServerCommandDistributedScope {

  private static final String[] NAMES = { "GET|commandCache/*", "PUT|commandCache/*" };

  public OServerCommandQueryCacheManager() {
    super("server.profiler");
  }

  @Override
  public boolean execute(OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    final String[] urlParts = checkSyntax(iRequest.url, 2, "Syntax error: commandCache/<database>");

    if (isLocalNode(iRequest)) {

      if ("GET".equals(iRequest.httpMethod)) {
        doGet(iRequest, iResponse, urlParts);
      } else if ("PUT".equals(iRequest.httpMethod)) {
        doPut(iRequest, iResponse, urlParts);
      } else {
        throw new IllegalArgumentException("Method " + iRequest.httpMethod + " not supported.");
      }
    } else {
      proxyRequest(iRequest, iResponse);
    }
    return false;
  }

  private void doPut(OHttpRequest iRequest, OHttpResponse iResponse, String[] urlParts) throws InterruptedException {

    iRequest.databaseName = urlParts[1];
    ODatabaseDocumentTx profiledDatabaseInstance = getProfiledDatabaseInstance(iRequest);
    OCommandCache commandCache = profiledDatabaseInstance.getMetadata().getCommandCache();

    if (urlParts.length == 2) {

      ODocument cfg = new ODocument().fromJSON(iRequest.content);

      
    } else {
      if (urlParts.length > 2) {
        String command = urlParts[2];

        if ("enable".equalsIgnoreCase(command)) {

          commandCache.enable();

        } else if ("disable".equalsIgnoreCase(command)) {

          commandCache.disable();

        }
      }
    }
  }

  private void doGet(OHttpRequest iRequest, OHttpResponse iResponse, String[] urlParts) throws InterruptedException, IOException {

    iRequest.databaseName = urlParts[1];
    ODatabaseDocumentTx profiledDatabaseInstance = getProfiledDatabaseInstance(iRequest);

    OCommandCache commandCache = profiledDatabaseInstance.getMetadata().getCommandCache();

    ODocument cache = new ODocument();
    cache.field("size", commandCache.size());
    cache.field("evictStrategy", commandCache.getEvictStrategy());
    cache.field("enabled", commandCache.isEnabled());
    cache.field("maxSize", commandCache.getMaxResultsetSize());

    cache.field("cachedResults");
    iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_JSON, cache.toJSON("prettyPrint"), null);

  }

  @Override
  public String[] getNames() {
    return NAMES;
  }
}
