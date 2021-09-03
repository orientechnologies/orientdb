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
package com.orientechnologies.orient.core.db.tool;

import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import java.util.Collections;
import java.util.List;

/**
 * Base class for tools related to databases.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public abstract class ODatabaseTool implements Runnable {
  protected OCommandOutputListener output;
  protected ODatabaseDocumentInternal database;
  protected boolean verbose = false;

  protected abstract void parseSetting(final String option, final List<String> items);

  protected void message(final String iMessage, final Object... iArgs) {
    if (output != null) output.onMessage(String.format(iMessage, iArgs));
  }

  public ODatabaseTool setOptions(final String iOptions) {
    if (iOptions != null) {
      final List<String> options = OStringSerializerHelper.smartSplit(iOptions, ' ');
      for (String o : options) {
        final int sep = o.indexOf('=');
        if (sep == -1) {
          parseSetting(o, Collections.EMPTY_LIST);
        } else {
          final String option = o.substring(0, sep);
          final String value = OIOUtils.getStringContent(o.substring(sep + 1));
          final List<String> items = OStringSerializerHelper.smartSplit(value, ' ');
          parseSetting(option, items);
        }
      }
    }
    return this;
  }

  public ODatabaseTool setOutputListener(final OCommandOutputListener iListener) {
    output = iListener;
    return this;
  }

  public ODatabaseTool setDatabase(final ODatabaseDocumentInternal database) {
    this.database = database;
    return this;
  }

  public ODatabaseTool setVerbose(final boolean verbose) {
    this.verbose = verbose;
    return this;
  }
}
