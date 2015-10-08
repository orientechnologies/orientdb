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

package com.orientechnologies.orient.etl;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import org.junit.After;
import org.junit.Before;

import java.util.List;

/**
 * Tests ETL JSON Extractor.
 *
 * @author Luca Garulli
 */
public abstract class ETLBaseTest {
  protected String[]      names    = new String[] { "Jay", "Luca", "Bill", "Steve", "Jill", "Luigi", "Enrico", "Emanuele" };
  protected String[]      surnames = new String[] { "Miner", "Ferguson", "Cancelli", "Lavori", "Raggio", "Eagles", "Smiles",
      "Ironcutter"                };

  protected OrientGraph   graph;
  protected OETLProcessor proc;

  @Before
  public void setUp() {
    graph = new OrientGraph("memory:ETLBaseTest");
    graph.setUseLightweightEdges(false);
    proc = new OETLProcessor();
    proc.getFactory().registerLoader(TestLoader.class);
  }

  @After
  public void tearDown() {
    graph.drop();
  }

  protected List<ODocument> getResult() {
    return ((TestLoader) proc.getLoader()).loadedRecords;
  }

  protected void process(final String cfgJson) {
    ODocument cfg = new ODocument().fromJSON(cfgJson, "noMap");
    proc.parse(cfg, null);
    proc.execute();
  }

  protected void process(final String cfgJson, final OCommandContext iContext) {
    ODocument cfg = new ODocument().fromJSON(cfgJson, "noMap");
    proc.parse(cfg, iContext);
    proc.execute();
  }
}
