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
package com.orientechnologies.orient.server.plugin;

import java.util.Collection;

import com.orientechnologies.orient.server.OClientConnection;
import com.orientechnologies.orient.server.OServer;

public class OServerPluginHelper {

  public static void invokeHandlerCallbackOnClientConnection(final OServer iServer, final OClientConnection connection) {
    final Collection<OServerPluginInfo> plugins = iServer.getPlugins();
    if (plugins != null)
      for (OServerPluginInfo plugin : plugins) {
        final OServerPlugin pluginInstance = plugin.getInstance();
        if (pluginInstance != null)
          plugin.getInstance().onClientConnection(connection);
      }
  }

  public static void invokeHandlerCallbackOnClientDisconnection(final OServer iServer, final OClientConnection connection) {
    final Collection<OServerPluginInfo> plugins = iServer.getPlugins();
    if (plugins != null)
      for (OServerPluginInfo plugin : plugins) {
        final OServerPlugin pluginInstance = plugin.getInstance();
        if (pluginInstance != null)
          pluginInstance.onClientDisconnection(connection);
      }
  }

  public static void invokeHandlerCallbackOnBeforeClientRequest(final OServer iServer, final OClientConnection connection,
      final byte iRequestType) {
    final Collection<OServerPluginInfo> plugins = iServer.getPlugins();
    if (plugins != null)
      for (OServerPluginInfo plugin : plugins) {
        final OServerPlugin pluginInstance = plugin.getInstance();
        if (pluginInstance != null)
          pluginInstance.onBeforeClientRequest(connection, iRequestType);
      }
  }

  public static void invokeHandlerCallbackOnAfterClientRequest(final OServer iServer, final OClientConnection connection,
      final byte iRequestType) {
    final Collection<OServerPluginInfo> plugins = iServer.getPlugins();
    if (plugins != null)
      for (OServerPluginInfo plugin : plugins) {
        final OServerPlugin pluginInstance = plugin.getInstance();
        if (pluginInstance != null)
          pluginInstance.onAfterClientRequest(connection, iRequestType);
      }
  }

  public static void invokeHandlerCallbackOnClientError(final OServer iServer, final OClientConnection connection,
      final Throwable iThrowable) {
    final Collection<OServerPluginInfo> plugins = iServer.getPlugins();
    if (plugins != null)
      for (OServerPluginInfo plugin : plugins) {
        final OServerPlugin pluginInstance = plugin.getInstance();
        if (pluginInstance != null)
          pluginInstance.onClientError(connection, iThrowable);
      }
  }

  protected OServerPluginHelper() {
  }

}
