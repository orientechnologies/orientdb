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
import com.tinkerpop.blueprints.impls.orient.OrientEdge;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

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

  public class OETLProcessorStats {
    public boolean verbose               = false;
    public long    lastExtractorProgress = 0;
    public long    lastLoaderProgress    = 0;
    public long    lastLap               = 0;
    public long    warnings              = 0;
    public long    errors                = 0;
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
      configureComponent(extractor, (ODocument) iExtractor.field(name));

      // TRANSFORMERS
      transformers = new ArrayList<OTransformer>();
      for (ODocument t : iTransformers) {
        name = t.fieldNames()[0];
        final OTransformer tr = factory.getTransformer(name);
        transformers.add(tr);
        configureComponent(tr, (ODocument) t.field(name));
      }

      // LOADER
      name = iLoader.fieldNames()[0];
      loader = factory.getLoader(name);
      configureComponent(loader, (ODocument) iLoader.field(name));

    } catch (Exception e) {
      throw new OConfigurationException("Error on creating ETL processor", e);
    }
  }

  public static void main(final String[] args) {
    String dbURL = null;
    String dbUser = "admin";
    String dbPassword = "admin";
    boolean verbose = false;
    boolean dbAutoCreate = true;

    ODocument cfgExtract = null;
    Collection<ODocument> cfgTransformers = null;
    ODocument cfgLoader = null;

    for (int i = 0; i < args.length; ++i) {
      final String arg = args[i];

      if (arg.equalsIgnoreCase("-dbUrl")) {
        dbURL = args[++i];
      } else if (arg.equalsIgnoreCase("-dbUser")) {
        dbUser = args[++i];
      } else if (arg.equalsIgnoreCase("-dbPassword")) {
        dbPassword = args[++i];
      } else if (arg.equalsIgnoreCase("-dbAutoCreate")) {
        dbAutoCreate = Boolean.parseBoolean(args[++i]);
      } else if (arg.equalsIgnoreCase("-v")) {
        verbose = true;
      } else if (arg.equalsIgnoreCase("-config")) {
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

    if (db.exists()) {
      db.open(dbUser, dbPassword);
    } else {
      if (dbAutoCreate) {
        db.create();
      } else {
        throw new IllegalArgumentException("Database '" + dbURL + "' not exists and 'dbAutoCreate' setting is false");
      }
    }

    final OETLProcessor processor = new OETLProcessor(cfgExtract, cfgTransformers, cfgLoader).setVerbose(verbose);

    processor.init(processor, db);
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
    analyzeFlow();

    final OCommandContext context = new OBasicCommandContext();

    try {
      begin(context);

      final String loaderIn = loader.getConfiguration().field("in");

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

    } catch (OETLProcessHaltedException e) {
      out(false, "ETL process halted: " + e);
    }
  }

  @Override
  public ODocument getConfiguration() {
    return null;
  }

  @Override
  public void configure(final ODocument iConfiguration) {
  }

  @Override
  public void init(OETLProcessor iProcessor, final ODatabaseDocumentTx iDatabase) {
    extractor.init(iProcessor, iDatabase);
    for (OTransformer t : transformers)
      t.init(iProcessor, iDatabase);
    loader.init(iProcessor, iDatabase);
  }

  @Override
  public String getName() {
    return "Processor";
  }

  public OETLProcessor setVerbose(final boolean verbose) {
    stats.verbose = verbose;
    return this;
  }

  public void out(final boolean iDebug, final String iText, final Object... iArgs) {
    if (!iDebug || stats.verbose)
      System.out.println(String.format(iText, iArgs));
  }

  public OETLProcessorStats getStats() {
    return stats;
  }

  protected void configureComponent(final OETLComponent iComponent, final ODocument iCfg) {
    iComponent.configure(iCfg);
  }

  protected Class getClassByName(final OETLComponent iComponent, final String iClassName) {
    final Class inClass;
    if (iClassName.equals("ODocument"))
      inClass = ODocument.class;
    else if (iClassName.equals("String"))
      inClass = String.class;
    else if (iClassName.equals("Object"))
      inClass = Object.class;
    else if (iClassName.equals("OrientVertex"))
      inClass = OrientVertex.class;
    else if (iClassName.equals("OrientEdge"))
      inClass = OrientEdge.class;
    else
      try {
        inClass = Class.forName(iClassName);
      } catch (ClassNotFoundException e) {
        throw new OConfigurationException("Class '" + iClassName + "' declared as 'input' of ETL Component '"
            + iComponent.getName() + "' was not found.");
      }
    return inClass;
  }

  protected void end(OCommandContext context) {
    elapsed = System.currentTimeMillis() - startTime;
    if (dumpTask != null) {
      dumpTask.cancel();
    }

    out(false, "END ETL PROCESSOR");

    dumpProgress(false);
  }

  protected void begin(OCommandContext context) {
    out(false, "BEGIN ETL PROCESSOR");

    if (dumpEveryMs > 0) {
      dumpTask = new TimerTask() {
        @Override
        public void run() {
          dumpProgress(true);
        }
      };

      Orient.instance().getTimer().schedule(dumpTask, dumpEveryMs, dumpEveryMs);

      startTime = System.currentTimeMillis();
    }
  }

  protected void dumpProgress(final boolean iDebug) {
    final long now = System.currentTimeMillis();

    final long extractorProgress = extractor.getProgress();
    final long extractorTotal = extractor.getTotal();
    final long extractorItemsSec = (long) ((extractorProgress - stats.lastExtractorProgress) * 1000f / (now - stats.lastLap));
    final String extractorUnit = extractor.getUnit();

    final long loaderProgress = loader.getProgress();
    final long loaderItemsSec = (long) ((loaderProgress - stats.lastLoaderProgress) * 1000f / (now - stats.lastLap));
    final String loaderUnit = loader.getUnit();

    out(iDebug,
        "+ %3.2f%% -> extracted %,d/%,d %s (%,d %s/sec) -> loaded %,d %s (%,d %s/sec) Total time: %s [%d warnings, %d errors]",
        ((float) extractorProgress * 100 / extractorTotal), extractorProgress, extractorTotal, extractorUnit, extractorItemsSec,
        extractorUnit, loaderProgress, loaderUnit, loaderItemsSec, loaderUnit, OIOUtils.getTimeAsString(now - startTime),
        stats.warnings, stats.errors);

    stats.lastExtractorProgress = extractorProgress;
    stats.lastLoaderProgress = loaderProgress;
    stats.lastLap = now;
  }

  protected void analyzeFlow() {
    if (extractor == null)
      throw new OConfigurationException("extractor is null");

    if (loader == null)
      throw new OConfigurationException("loader is null");

    OETLComponent lastComponent = extractor;

    for (OTransformer t : transformers) {
      checkTypeCompatibility(t, lastComponent);
      lastComponent = t;
    }

    checkTypeCompatibility(loader, lastComponent);
  }

  protected void checkTypeCompatibility(OETLComponent iCurrentComponent, OETLComponent iLastComponent) {
    final String out = iLastComponent.getConfiguration().field("output");
    final Class outClass = getClassByName(iLastComponent, out);

    final List<String> ins = iCurrentComponent.getConfiguration().field("input");
    for (String in : ins) {
      final Class inClass = getClassByName(iCurrentComponent, in);
      if (inClass.isAssignableFrom(outClass)) {
        return;
      }
    }

    throw new OConfigurationException("Component " + iCurrentComponent.getName() + " expects one of the following inputs " + ins
        + " but the 'output' for component '" + iLastComponent.getName() + "' is: " + out);
  }
}
