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

package com.orientechnologies.agent.proxy;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Created by Enrico Risa on 19/11/15.
 */
public class HttpBroadcastCallable implements Callable<HttpProxyResponse> {

  private HttpProxy                 proxy;
  private final OHttpRequest        request;
  private ODistributedServerManager manager;
  private final ODocument           member;

  public HttpBroadcastCallable(HttpProxy proxy, OHttpRequest request, ODistributedServerManager manager, ODocument member) {
    this.proxy = proxy;
    this.request = request;
    this.manager = manager;
    this.member = member;
  }

  @Override
  public HttpProxyResponse call() throws Exception {
    final HttpProxyResponse httpProxyResponse = new HttpProxyResponse();

    proxy.fetchFromServer(manager, member, request, bindParameters(request, member), null, new HttpProxyListener() {
      @Override
      public void onProxySuccess(OHttpRequest request, OHttpResponse response, InputStream is) throws IOException {
        httpProxyResponse.setStream(is);
      }

      @Override
      public void onProxyError(OHttpRequest request, OHttpResponse response, InputStream is, int code, Exception e)
          throws IOException {

        httpProxyResponse.setName((String) member.field("name"));
        httpProxyResponse.setStream(is);
        httpProxyResponse.setCode(code);

      }
    });
    return httpProxyResponse;
  }

  protected Map<String, String> bindParameters(final OHttpRequest request, final ODocument member) {

    final String name = member.field("name");
    return new HashMap<String, String>() {
      {
        putAll(request.getParameters());
        put("node", name);
      }
    };

  }
}
