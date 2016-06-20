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

import com.orientechnologies.orient.core.cache.OCommandCacheSoftRefs;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Created by Enrico Risa on 21/11/15.
 */
public class OServerCommandQueryCacheManager extends OServerCommandDistributedScope {

    private static final String[] NAMES = { "GET|commandCache/*", "PUT|commandCache/*", "POST|commandCache/*" };

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
            } else if ("POST".equals(iRequest.httpMethod)) {
                doPost(iRequest, iResponse, urlParts);
            } else {
                throw new IllegalArgumentException("Method " + iRequest.httpMethod + " not supported.");
            }
        } else {
            proxyRequest(iRequest, iResponse);
        }
        return false;
    }

    private void doPost(OHttpRequest iRequest, OHttpResponse iResponse, String[] urlParts) throws InterruptedException, IOException {

        if (urlParts.length > 2) {
            String command = urlParts[2];
            if ("results".equalsIgnoreCase(command)) {
                iRequest.databaseName = urlParts[1];
                ODatabaseDocument profiledDatabaseInstance = getProfiledDatabaseInstance(iRequest);
                OCommandCacheSoftRefs commandCache = (OCommandCacheSoftRefs) profiledDatabaseInstance.getMetadata().getCommandCache();
                ODocument query = new ODocument().fromJSON(iRequest.content);
                String q = query.field("query");

                int userPos = q.indexOf(".");
                int limitPos = q.lastIndexOf(".");

                String user = q.substring(0, userPos);
                String realQuery = q.substring(userPos + 1, limitPos);
                String limit = q.substring(limitPos + 1, q.length());

                OUser ouser = new OUser(user);
                Object results = commandCache.get(ouser, realQuery, Integer.parseInt(limit));

                iResponse.writeResult(results);

            } else if ("purge".equalsIgnoreCase(command)) {

                iRequest.databaseName = urlParts[1];
                ODatabaseDocument profiledDatabaseInstance = getProfiledDatabaseInstance(iRequest);
                OCommandCacheSoftRefs commandCache = (OCommandCacheSoftRefs) profiledDatabaseInstance.getMetadata().getCommandCache();
                commandCache.clear();
                iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_JSON, null, null);
            } else {
                throw new IllegalArgumentException("Method " + iRequest.httpMethod + " not supported.");
            }

        }
    }

    private void doPut(OHttpRequest iRequest, OHttpResponse iResponse, String[] urlParts) throws InterruptedException, IOException {

        iRequest.databaseName = urlParts[1];
        ODatabaseDocument profiledDatabaseInstance = getProfiledDatabaseInstance(iRequest);
        OCommandCacheSoftRefs commandCache = (OCommandCacheSoftRefs) profiledDatabaseInstance.getMetadata().getCommandCache();

        if (urlParts.length == 2) {

            ODocument cfg = new ODocument().fromJSON(iRequest.content);

            commandCache.changeConfig(cfg);

            iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_JSON, null, null);
        } else {
            if (urlParts.length > 2) {
                String command = urlParts[2];

                if ("enable".equalsIgnoreCase(command)) {

                    commandCache.enable();
                    iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_JSON, null, null);
                } else if ("disable".equalsIgnoreCase(command)) {

                    commandCache.disable();
                    iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_JSON, null, null);
                }
            }
        }
    }

    private void doGet(OHttpRequest iRequest, OHttpResponse iResponse, String[] urlParts) throws InterruptedException, IOException {

        iRequest.databaseName = urlParts[1];

        ODatabaseDocument profiledDatabaseInstance = getProfiledDatabaseInstance(iRequest);

        OCommandCacheSoftRefs commandCache = (OCommandCacheSoftRefs) profiledDatabaseInstance.getMetadata().getCommandCache();
        if (urlParts.length > 2 && urlParts[2].equalsIgnoreCase("results")) {
            ODocument cache = new ODocument();
            Collection<ODocument> results = new ArrayList<ODocument>();
            cache.field("results", results);

            Set<Map.Entry<String, OCommandCacheSoftRefs.OCachedResult>> entries = commandCache.entrySet();

            for (Map.Entry<String, OCommandCacheSoftRefs.OCachedResult> entry : entries) {
                ODocument doc = new ODocument();
                doc.field("query", entry.getKey());
                Object result = entry.getValue().getResult();
                int size = 1;
                if (result instanceof Collection) {
                    size = ((Collection) result).size();
                }
                doc.field("size", size);
                results.add(doc);
            }
            iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_JSON, cache.toJSON(), null);
        } else {

            ODocument cache = new ODocument();
            cache.field("size", commandCache.size());
            cache.field("evictStrategy", commandCache.getEvictStrategy());
            cache.field("enabled", commandCache.isEnabled());
            cache.field("maxResultsetSize", commandCache.getMaxResultsetSize());
            cache.field("minExecutionTime", commandCache.getMinExecutionTime());

            iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_JSON, cache.toJSON(), null);
        }

    }

    @Override
    public String[] getNames() {
        return NAMES;
    }
}

