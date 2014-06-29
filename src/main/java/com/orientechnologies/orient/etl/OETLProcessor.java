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

import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.etl.extract.OExtractor;
import com.orientechnologies.orient.etl.loader.OLoader;
import com.orientechnologies.orient.etl.transform.OTransformer;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.TimerTask;

/**
 * ETL processor class.
 * 
 * @author Luca Garulli (l.garulli-at-orientechnologies.com)
 */
public class OETLProcessor implements OETLComponent {
  protected final OExtractor           extractor;
  protected final List<OTransformer>   transformers;
  protected final OLoader              loader;
  protected final OETLComponentFactory factory     = new OETLComponentFactory();
  protected long                       startTime;
  protected long                       elapsed;

  protected long                       dumpEveryMs = 1000;
  protected OETLProcessorStats         stats       = new OETLProcessorStats();
  protected TimerTask                  dumpTask;

  protected class OETLProcessorStats {
    public long lastProgress = 0;
    public long lastLap      = 0;
    public long warnings     = 0;
    public long errors       = 0;
  }

  public OETLProcessor(final OExtractor iExtractor, final OTransformer[] iTransformers, final OLoader iLoader) {
    extractor = iExtractor;
    transformers = Arrays.asList(iTransformers);
    loader = iLoader;
  }

  public OETLProcessor(final OExtractor iExtractor, final List<OTransformer> iTransformers, final OLoader iLoader) {
    extractor = iExtractor;
    transformers = iTransformers;
    loader = iLoader;
  }

  public OETLProcessor(final ODocument iExtractor, final Collection<ODocument> iTransformers, final ODocument iLoader) {
    try {
      // EXTRACTOR
      String name = iExtractor.fieldNames()[0];
      extractor = factory.getExtractor(name);
      extractor.configure((ODocument) iExtractor.field(name));

      // TRANSFORMERS
      transformers = new ArrayList<OTransformer>();
      for (ODocument t : iTransformers) {
        name = t.fieldNames()[0];
        final OTransformer tr = factory.getTransformer(name);
        transformers.add(tr);
        tr.configure((ODocument) t.field(name));
      }

      // LOADER
      name = iLoader.fieldNames()[0];
      loader = factory.getLoader(name);
      loader.configure((ODocument) iLoader.field(name));

    } catch (Exception e) {
      throw new OConfigurationException("Error on creating ETL perocessor", e);
    }
  }

  public static void main(final String[] args) {
    String dbURL = null;
    String dbUser = "admin";
    String dbPassword = "admin";
    boolean dbAutoCreate = true;

    ODocument cfgExtract = null;
    Collection<ODocument> cfgTransformers = null;
    ODocument cfgLoader = null;

    for (int i = 0; i < args.length; ++i) {
      final String arg = args[i];

      if (arg.equalsIgnoreCase("-dbUrl"))
        dbURL = args[++i];
      else if (arg.equalsIgnoreCase("-dbUser"))
        dbUser = args[++i];
      else if (arg.equalsIgnoreCase("-dbPassword"))
        dbPassword = args[++i];
      else if (arg.equalsIgnoreCase("-dbAutoCreate"))
        dbAutoCreate = Boolean.parseBoolean(args[++i]);
      else if (arg.equalsIgnoreCase("-config")) {
        final String cfgPath = args[++i];
        try {
          final String config = OIOUtils.readFileAsString(new File(cfgPath));
          final ODocument cfg = new ODocument().fromJSON(config, "noMap");

          cfgExtract = cfg.field("extractor");
          cfgTransformers = cfg.field("transformers");
          cfgLoader = cfg.field("loader");

        } catch (IOException e) {
          throw new OConfigurationException("Error on loading config file: " + cfgPath);
        }
      } else if (arg.equalsIgnoreCase("-e")) {
        cfgExtract = new ODocument().fromJSON(args[++i], "noMap");
      } else if (arg.equalsIgnoreCase("-t")) {
        final String value = args[++i];
        cfgTransformers = parseTransformers(value);

      } else if (arg.equalsIgnoreCase("-l")) {
        cfgLoader = new ODocument().fromJSON(args[++i], "noMap");
      }
    }

    if (cfgExtract == null)
      throw new IllegalArgumentException("No Extractor configured");

    if (cfgTransformers == null)
      throw new IllegalArgumentException("No Transformer configured");

    if (cfgLoader == null)
      throw new IllegalArgumentException("No Loader configured");

    if (dbURL == null)
      throw new IllegalArgumentException("Argument dbURL not found");

    final ODatabaseDocumentTx db = new ODatabaseDocumentTx(dbURL);

    if (db.exists())
      db.open(dbUser, dbPassword);
    else {
      if (dbAutoCreate)
        db.create();
      else
        throw new IllegalArgumentException("Database '" + dbURL + "' not exists and 'dbAutoCreate' setting is false");
    }

    final OETLProcessor processor = new OETLProcessor(cfgExtract, cfgTransformers, cfgLoader);

    processor.prepare(db);
    processor.execute();
  }

  protected static Collection<ODocument> parseTransformers(final String value) {
    final ArrayList<ODocument> cfgTransformers = new ArrayList<ODocument>();
    if (!value.isEmpty()) {
      if (value.charAt(0) == '{') {
        cfgTransformers.add((ODocument) new ODocument().fromJSON(value, "noMap"));
      } else if (value.charAt(0) == '[') {
        final ArrayList<String> items = new ArrayList<String>();
        OStringSerializerHelper.getCollection(value, 0, items);
        for (String item : items)
          cfgTransformers.add((ODocument) new ODocument().fromJSON(item, "noMap"));
      }
    }
    return cfgTransformers;
  }

  public void execute() {

    final OCommandContext context = new OBasicCommandContext();

    begin(context);

    extractor.extract(context);

    Object current = null;
    while (extractor.hasNext()) {
      // EXTRACTOR
      current = extractor.next();

      // TRANSFORM
      for (OTransformer t : transformers) {
        current = t.transform(current, context);
        if (current == null)
          break;
      }

      if (current != null)
        // LOAD
        loader.load(current, context);
    }

    end(context);
  }

  @Override
  public void configure(final ODocument iConfiguration) {
  }

  @Override
  public void prepare(final ODatabaseDocumentTx iDatabase) {
    extractor.prepare(iDatabase);
    for (OTransformer t : transformers)
      t.prepare(iDatabase);
    loader.prepare(iDatabase);
  }

  @Override
  public String getName() {
    return "Processor";
  }

  protected void end(OCommandContext context) {
    elapsed = System.currentTimeMillis() - startTime;
    if (dumpTask != null) {
      dumpTask.cancel();
    }

    OLogManager.instance().info(this, "COMPLETED");

    dumpProgress();
  }

  protected void begin(OCommandContext context) {
    OLogManager.instance().info(this, "BEGIN");

    if (dumpEveryMs > 0) {
      dumpTask = new TimerTask() {
        @Override
        public void run() {
          dumpProgress();
        }
      };

      Orient.instance().getTimer().schedule(dumpTask, dumpEveryMs, dumpEveryMs);

      startTime = System.currentTimeMillis();
    }
  }

  protected void dumpProgress() {
    final long now = System.currentTimeMillis();
    final long extractorProgress = extractor.getProgress();
    final long extractorTotal = extractor.getTotal();

    final long extractorItemsSec = (long) ((extractorProgress - stats.lastProgress) * 1000f / (now - stats.lastLap));

    OLogManager.instance().info(this,
        "+ %3.2f%% -> extracted %,d/%,d (%,d items/sec) -> transformed -> loaded %,d [%d warnings, %d errors]",
        ((float) extractorProgress * 100 / extractorTotal), extractorProgress, extractorTotal, extractorItemsSec,
        loader.getProgress(), stats.warnings, stats.errors);

    stats.lastProgress = extractorProgress;
    stats.lastLap = now;
  }
}
