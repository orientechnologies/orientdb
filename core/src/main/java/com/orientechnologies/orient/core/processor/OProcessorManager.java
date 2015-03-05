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
package com.orientechnologies.orient.core.processor;

import com.orientechnologies.common.factory.ODynamicFactory;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.record.impl.ODocument;

public class OProcessorManager extends ODynamicFactory<String, OProcessor> {
  private static OProcessorManager instance = new OProcessorManager();

  public static OProcessorManager getInstance() {
    return instance;
  }

  public Object process(final String iType, final Object iContent, final OCommandContext iContext, ODocument iOutput,
      final boolean iReadOnly) {
    final OProcessor t = registry.get(iType);
    if (t == null) {
        throw new OProcessException("Cannot find processor type '" + iType + "'");
    }

    return t.process(null, iContent, iContext, iOutput, iReadOnly);
  }
}
