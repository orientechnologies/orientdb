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

package com.orientechnologies.orient.etl.loader;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.etl.OAbstractETLComponent;

/**
 * ETL Loader that saves record into OrientDB database.
 */
public class OOutputLoader extends OAbstractETLComponent implements OLoader {
  protected long progress = 0;

  @Override
  public void load(final Object input, final OCommandContext context) {
    progress++;
    System.out.println(input);
  }

  @Override
  public long getProgress() {
    return progress;
  }

  @Override
  public String getUnit() {
    return "bytes";
  }

  @Override
  public ODocument getConfiguration() {
    return new ODocument();
  }

  @Override
  public String getName() {
    return "output";
  }
}
