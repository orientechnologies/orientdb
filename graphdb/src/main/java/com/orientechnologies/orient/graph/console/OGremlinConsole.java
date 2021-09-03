/*
 * Copyright 2010-2014 OrientDB LTD (info--at--orientdb.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.graph.console;

import com.orientechnologies.common.console.TTYConsoleReader;
import com.orientechnologies.common.console.annotation.ConsoleCommand;
import com.orientechnologies.common.console.annotation.ConsoleParameter;
import com.orientechnologies.orient.console.OConsoleDatabaseApp;
import com.orientechnologies.orient.core.command.OCommandExecutorNotFoundException;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.tool.ODatabaseImportException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.graph.graphml.OGraphMLReader;
import com.orientechnologies.orient.graph.graphml.OGraphSONReader;
import com.orientechnologies.orient.graph.gremlin.OCommandGremlin;
import com.orientechnologies.orient.graph.gremlin.OGremlinHelper;
import com.tinkerpop.blueprints.impls.orient.OBonsaiTreeRepair;
import com.tinkerpop.blueprints.impls.orient.OGraphRepair;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.util.io.graphml.GraphMLWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipInputStream;

/**
 * Gremlin specialized console.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OGremlinConsole extends OConsoleDatabaseApp {

  public OGremlinConsole(final String[] args) {
    super(args);
  }

  public static void main(final String[] args) {
    int result;
    final boolean interactiveMode = isInteractiveMode(args);
    try {
      boolean tty = false;
      try {
        if (setTerminalToCBreak(interactiveMode)) tty = true;

      } catch (Exception e) {
      }

      final OConsoleDatabaseApp console = new OGremlinConsole(args);
      if (tty) console.setReader(new TTYConsoleReader(console.historyEnabled()));

      result = console.run();

    } finally {
      try {
        stty("echo", interactiveMode);
      } catch (Exception e) {
      }
    }
    System.exit(result);
  }

  @ConsoleCommand(splitInWords = false, description = "Execute a GREMLIN script")
  public void gremlin(
      @ConsoleParameter(name = "script-text", description = "The script text to execute")
          final String iScriptText) {
    checkForDatabase();

    if (iScriptText == null || iScriptText.length() == 0) return;

    resetResultSet();

    long start = System.currentTimeMillis();
    try {
      final Object result = currentDatabase.command(new OCommandGremlin(iScriptText)).execute();

      float elapsed = System.currentTimeMillis() - start;

      out.println("\n" + result);

      out.printf("\nScript executed in %f ms.", elapsed);
    } catch (OStorageException e) {
      final Throwable cause = e.getCause();
      if (cause instanceof OCommandExecutorNotFoundException)
        out.printf(
            "\nError: the GREMLIN command executor is not installed, check your configuration");
    }
  }

  @Override
  @ConsoleCommand(description = "Import a database into the current one", splitInWords = false)
  public void importDatabase(
      @ConsoleParameter(name = "options", description = "Import options") String text)
      throws IOException {
    checkForDatabase();

    final List<String> items = OStringSerializerHelper.smartSplit(text, ' ');
    final String fileName =
        items.size() <= 0 || (items.get(1)).charAt(0) == '-' ? null : items.get(1);
    final String optionsAsString =
        fileName != null
            ? text.substring((items.get(0)).length() + (items.get(1)).length() + 1).trim()
            : text;

    final Map<String, List<String>> options = parseOptions(optionsAsString);

    final String format = options.containsKey("-format") ? options.get("-format").get(0) : null;

    if ((format != null && format.equalsIgnoreCase("graphml"))
        || (fileName != null && (fileName.endsWith(".graphml") || fileName.endsWith(".xml")))) {
      // GRAPHML
      message(
          "\nImporting GRAPHML database from "
              + fileName
              + " with options ("
              + optionsAsString
              + ")...");

      final OrientGraph g =
          (OrientGraph) OrientGraphFactory.getTxGraphImplFactory().getGraph(currentDatabase);
      try {
        g.setUseLog(false);
        g.setWarnOnForceClosingTx(false);

        final long totalEdges = g.countEdges();
        final long totalVertices = g.countVertices();

        final File file = new File(fileName);
        if (!file.exists())
          throw new ODatabaseImportException("Input file '" + fileName + "' not exists");

        InputStream is = new FileInputStream(file);
        if (fileName.endsWith(".zip")) is = new ZipInputStream(is);
        else if (fileName.endsWith(".gz")) is = new GZIPInputStream(is);

        try {
          new OGraphMLReader(g)
              .setOptions(options)
              .setOutput(
                  new OCommandOutputListener() {
                    @Override
                    public void onMessage(final String iText) {
                      System.out.print("\r" + iText);
                    }
                  })
              .inputGraph(is);
          g.commit();
          currentDatabase.commit();

          message(
              "\nDone: imported %d vertices and %d edges",
              g.countVertices() - totalVertices, g.countEdges() - totalEdges);

        } finally {
          is.close();
        }

      } catch (ODatabaseImportException e) {
        printError(e);
      } finally {
        g.shutdown(false, true);
      }
    } else if ((format != null && format.equalsIgnoreCase("graphson"))
        || (fileName != null && (fileName.endsWith(".graphson")))) {
      // GRAPHSON
      message(
          "\nImporting GRAPHSON database from "
              + fileName
              + " with options ("
              + optionsAsString
              + ")...");

      try {
        final OrientGraph g =
            (OrientGraph) OrientGraphFactory.getTxGraphImplFactory().getGraph(currentDatabase);
        g.setUseLog(false);
        g.setWarnOnForceClosingTx(false);

        final long totalEdges = g.countEdges();
        final long totalVertices = g.countVertices();

        final File file = new File(fileName);
        if (!file.exists())
          throw new ODatabaseImportException("Input file '" + fileName + "' not exists");

        InputStream is = new FileInputStream(file);
        if (fileName.endsWith(".zip")) {
          is = new ZipInputStream(is);
        } else if (fileName.endsWith(".gz")) {
          is = new GZIPInputStream(is);
        }

        try {
          new OGraphSONReader(g)
              .setOutput(
                  new OCommandOutputListener() {
                    @Override
                    public void onMessage(final String iText) {
                      System.out.print("\r" + iText);
                    }
                  })
              .inputGraph(is, 10000);

          // new OGraphMLReader(g).setOptions(options).inputGraph(g, fileName);
          g.commit();
          currentDatabase.commit();

          message(
              "\nDone: imported %d vertices and %d edges",
              g.countVertices() - totalVertices, g.countEdges() - totalEdges);

        } finally {
          is.close();
        }

      } catch (ODatabaseImportException e) {
        printError(e);
      }
    } else if (format == null) super.importDatabase(text);
    else throw new IllegalArgumentException("Format '" + format + "' is not supported");
  }

  @Override
  @ConsoleCommand(
      description = "Export a database",
      splitInWords = false,
      onlineHelp = "Console-Command-Export")
  public void exportDatabase(
      @ConsoleParameter(name = "options", description = "Export options") String iText)
      throws IOException {
    checkForDatabase();

    final List<String> items = OStringSerializerHelper.smartSplit(iText, ' ');
    final String fileName =
        items.size() <= 1 || items.get(1).charAt(0) == '-' ? null : items.get(1);
    if (fileName != null && (fileName.endsWith(".graphml") || fileName.endsWith(".xml"))) {
      message("\nExporting database in GRAPHML format to " + iText + "...");

      final OrientGraph g =
          (OrientGraph) OrientGraphFactory.getTxGraphImplFactory().getGraph(currentDatabase);
      try {
        g.setUseLog(false);
        g.setWarnOnForceClosingTx(false);

        // CREATE THE EXPORT FILE IF NOT EXIST YET
        final File f = new File(fileName);

        if (f.getParentFile() != null) {
          f.getParentFile().mkdirs();
        }
        f.createNewFile();

        new GraphMLWriter(g).outputGraph(fileName);

      } catch (ODatabaseImportException e) {
        printError(e);
      } finally {
        g.shutdown(false, true);
      }
    } else
      // BASE EXPORT
      super.exportDatabase(iText);
  }

  @Override
  @ConsoleCommand(description = "Check database integrity", splitInWords = false)
  public void checkDatabase(
      @ConsoleParameter(name = "options", description = "Options: -v --skip-graph", optional = true)
          final String iOptions)
      throws IOException {
    final boolean fix_graph = iOptions == null || !iOptions.contains("--skip-graph");
    if (fix_graph) {
      // REPAIR GRAPH
      final Map<String, List<String>> options = parseOptions(iOptions);
      new OGraphRepair()
          .check(
              OrientGraphFactory.getNoTxGraphImplFactory().getGraph(currentDatabase),
              this,
              options);
    }

    super.checkDatabase(iOptions);
  }

  @Override
  @ConsoleCommand(description = "Repair database structure", splitInWords = false)
  public void repairDatabase(
      @ConsoleParameter(
              name = "options",
              description =
                  "Options: [--fix-graph] [--force-embedded-ridbags] [--fix-links] [-v]] [--fix-ridbags] [--fix-bonsai]",
              optional = true)
          String iOptions)
      throws IOException {
    checkForDatabase();
    final boolean force_embedded =
        iOptions == null || iOptions.contains("--force-embedded-ridbags");
    final boolean fix_graph = iOptions == null || iOptions.contains("--fix-graph");
    if (force_embedded) {
      OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(Integer.MAX_VALUE);
      OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(Integer.MAX_VALUE);
    }
    if (fix_graph || force_embedded) {
      // REPAIR GRAPH
      final Map<String, List<String>> options = parseOptions(iOptions);
      new OGraphRepair()
          .repair(
              OrientGraphFactory.getNoTxGraphImplFactory().getGraph(currentDatabase),
              this,
              options);
    }

    final boolean fix_links = iOptions == null || iOptions.contains("--fix-links");
    if (fix_links) {
      // REPAIR DATABASE AT LOW LEVEL
      super.repairDatabase(iOptions);
    }

    if (!currentDatabase.getURL().startsWith("plocal")) {
      message("\n fix-bonsai can be run only on plocal connection \n");
      return;
    }

    final boolean fix_ridbags = iOptions == null || iOptions.contains("--fix-ridbags");
    final boolean fix_bonsai = iOptions == null || iOptions.contains("--fix-bonsai");
    if (fix_ridbags || fix_bonsai || force_embedded) {
      OBonsaiTreeRepair repairer = new OBonsaiTreeRepair();
      repairer.repairDatabaseRidbags(currentDatabase, this);
    }
  }

  @Override
  protected void onBefore() {
    super.onBefore();

    out.println(
        "\nInstalling extensions for GREMLIN language v." + OGremlinHelper.getEngineVersion());

    OGremlinHelper.global().create();
  }

  @Override
  protected boolean isCollectingCommands(final String iLine) {
    return super.isCollectingCommands(iLine) || iLine.trim().equalsIgnoreCase("gremlin");
  }
}
