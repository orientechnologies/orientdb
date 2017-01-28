package com.orientechnologies.orient.etl;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.etl.block.OETLBlock;
import com.orientechnologies.orient.etl.extractor.OETLExtractor;
import com.orientechnologies.orient.etl.loader.OETLLoader;
import com.orientechnologies.orient.etl.source.OETLSource;
import com.orientechnologies.orient.etl.transformer.OETLTransformer;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * Created by frank on 17/06/2016.
 */
public class OETLProcessorConfigurator {
  private final OETLComponentFactory factory;

  public OETLProcessorConfigurator() {
    this(new OETLComponentFactory());
  }

  public OETLProcessorConfigurator(OETLComponentFactory factory) {
    this.factory = factory;
  }

  protected OCommandContext createDefaultContext() {
    final OCommandContext context = new OBasicCommandContext();
    context.setVariable("dumpEveryMs", 1000);
    return context;
  }

  public OETLProcessor parseConfigAndParameters(String[] args) {
    final OCommandContext context = createDefaultContext();

    ODocument configuration = new ODocument().fromJSON("{}");
    for (final String arg : args) {
      if (!arg.startsWith("-")) {
        try {
          final String config = OIOUtils.readFileAsString(new File(arg));

          configuration.merge(new ODocument().fromJSON(config, "noMap"), true, true);
          ODocument cfgGlobal = configuration.field("config");
          if (cfgGlobal != null) {
            for (String f : cfgGlobal.fieldNames()) {
              context.setVariable(f, cfgGlobal.field(f));
            }
          }
        } catch (IOException e) {
          throw OException.wrapException(new OConfigurationException("Error on loading config file: " + arg), e);
        }
      }
    }

    // override variables with args passed by command line
    for (final String arg : args) {
      if (arg.startsWith("-")) {
        final String[] parts = arg.substring(1).split("=");
        context.setVariable(parts[0], parts[1]);
      }
    }

    return parse(configuration, context);
  }

  public OETLProcessor parse(final ODocument cfg, final OCommandContext context) {

    if (cfg.<ODocument>field("extractor") == null)
      throw new IllegalArgumentException("No Extractor configured");

    try {
      List<OETLBlock> beginBlocks = configureBeginBlocks(cfg, context);

      OETLSource source = configureSource(cfg, context);

      OETLExtractor extractor = configureExtractor(cfg, context);

      //copy  the skipDuplicates flag from vertex transformer to Loader
      Optional.ofNullable(cfg.<Collection<ODocument>>field("transformers"))
          .map(c -> c.stream()
              .filter(d -> d.containsField("vertex"))
              .map(d -> d.<ODocument>field("vertex"))
              .filter(d -> d.containsField("skipDuplicates"))
              .map(d -> d.field("skipDuplicates"))
              .map(skip -> cfg.<ODocument>field("loader")
                  .<ODocument>field(cfg.<ODocument>field("loader").fieldNames()[0])
                  .field("skipDuplicates", skip))
              .count());

      OETLLoader loader = configureLoader(cfg, context);

      List<OETLTransformer> transformers = configureTransformers(cfg, context);

      List<OETLBlock> endBlocks = configureEndBlocks(cfg, context);

      // isn't working right now
      // analyzeFlow();

      OETLProcessor processor = new OETLProcessor(beginBlocks, source, extractor, transformers, loader, endBlocks, context);

      List<OETLComponent> components = new ArrayList<OETLComponent>() {{
        add(source);
        addAll(transformers);
        addAll(beginBlocks);
        addAll(endBlocks);
        add(loader);
        add(extractor);
      }};

      components.stream().forEach(c -> c.setProcessor(processor));

      return processor;
    } catch (Exception e) {
      throw OException.wrapException(new OConfigurationException("Error on creating ETL processor"), e);
    }
  }

  protected void configureComponent(final OETLComponent iComponent, final ODocument iCfg, final OCommandContext iContext) {
    iComponent.configure(iCfg, iContext);
  }

  private List<OETLBlock> configureEndBlocks(ODocument cfg, OCommandContext iContext)

      throws IllegalAccessException, InstantiationException {
    List<OETLBlock> endBlocks = new ArrayList<OETLBlock>();
    Collection<ODocument> endBlocksConf = cfg.field("end");
    if (endBlocksConf != null) {
      for (ODocument blockConf : endBlocksConf) {
        final String name = blockConf.fieldNames()[0];
        final OETLBlock block = factory.getBlock(name);
        endBlocks.add(block);
        configureComponent(block, blockConf.<ODocument>field(name), iContext);
      }
    }
    return endBlocks;
  }

  private List<OETLTransformer> configureTransformers(ODocument cfg, OCommandContext iContext)
      throws IllegalAccessException, InstantiationException {
    Collection<ODocument> transformersConf = cfg.<Collection<ODocument>>field("transformers");
    List<OETLTransformer> transformers = new ArrayList<OETLTransformer>();
    if (transformersConf != null) {
      for (ODocument t : transformersConf) {
        String name = t.fieldNames()[0];
        final OETLTransformer tr = factory.getTransformer(name);
        transformers.add(tr);
        configureComponent(tr, t.<ODocument>field(name), iContext);
      }
    }
    return transformers;
  }

  private OETLLoader configureLoader(ODocument cfg, OCommandContext iContext)
      throws IllegalAccessException, InstantiationException {
    ODocument loadersConf = cfg.field("loader");
    if (loadersConf != null) {
      // LOADER
      String name = loadersConf.fieldNames()[0];
      OETLLoader loader = factory.getLoader(name);
      configureComponent(loader, loadersConf.<ODocument>field(name), iContext);
      return loader;
    }

    OETLLoader loader = factory.getLoader("output");
    configureComponent(loader, new ODocument(), iContext);

    return loader;

  }

  private OETLExtractor configureExtractor(ODocument cfg, OCommandContext iContext)
      throws IllegalAccessException, InstantiationException {
    // EXTRACTOR
    ODocument extractorConf = cfg.<ODocument>field("extractor");
    String name = extractorConf.fieldNames()[0];
    OETLExtractor extractor = factory.getExtractor(name);
    configureComponent(extractor, extractorConf.<ODocument>field(name), iContext);
    return extractor;
  }

  private OETLSource configureSource(ODocument cfg, OCommandContext iContext)
      throws IllegalAccessException, InstantiationException {

    ODocument sourceConf = cfg.field("source");
    if (sourceConf != null) {
      // SOURCE
      String name = sourceConf.fieldNames()[0];
      OETLSource source = factory.getSource(name);
      configureComponent(source, sourceConf.<ODocument>field(name), iContext);
      return source;
    }

    OETLSource source = factory.getSource("input");
    configureComponent(source, new ODocument(), iContext);
    return source;

  }

  private List<OETLBlock> configureBeginBlocks(ODocument cfg, OCommandContext iContext)
      throws IllegalAccessException, InstantiationException {

    Collection<ODocument> iBeginBlocks = cfg.field("begin");
    List<OETLBlock> blocks = new ArrayList<OETLBlock>();
    if (iBeginBlocks != null) {
      for (ODocument block : iBeginBlocks) {
        final String name = block.fieldNames()[0];// BEGIN BLOCKS
        final OETLBlock b = factory.getBlock(name);
        blocks.add(b);
        configureComponent(b, block.<ODocument>field(name), iContext);
        // Execution is necessary to resolve let blocks and provide resolved variables to other components
        b.execute();
      }
    }
    return blocks;
  }

}
