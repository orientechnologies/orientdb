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

package com.orientechnologies.agent.event;

import com.orientechnologies.agent.event.metric.OEventMetricExecutor;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.plugin.mail.OMailPlugin;

import javax.mail.MessagingException;
import java.util.Map;

@EventConfig(when = "MetricWhen", what = "MailWhat")
public class OEventMetricMailExecutor extends OEventMetricExecutor {
  private OMailPlugin mailPlugin;

  public OEventMetricMailExecutor() {

  }

  @Override
  public void execute(ODocument source, ODocument when, ODocument what) {

    if (canExecute(source, when)) {
      Map<String, Object> params = fillMapResolve(source, when);
      mailEvent(what, params);
    }
  }

  public void mailEvent(ODocument what, Map<String, Object> params) {
    if (mailPlugin == null) {
      mailPlugin = OServerMain.server().getPluginByClass(OMailPlugin.class);
    }

    Map<String, Object> configuration = EventHelper.createConfiguration(what, params);

    try {

      OLogManager.instance().debug(this, "EMAIL sending email: %s", configuration);

      mailPlugin.send(configuration);
    } catch (MessagingException e) {

      OLogManager.instance().error(this, "Error sending  email with configuration: %s", configuration);

    } catch (Exception e) {
      OLogManager.instance().error(this, "Error sending  email with configuration: %s", configuration);
    }

  }

}
