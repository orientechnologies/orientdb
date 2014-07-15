/*
 *
 *  * Copyright 2014 Orient Technologies.
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

package com.orientechnologies.test;

import java.io.*;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.shape.Shape;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.Version;
import org.testng.annotations.Test;

import au.com.bytecode.opencsv.CSVReader;

import com.orientechnologies.common.test.SpeedTestMonoThread;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Created by enricorisa on 07/04/14.
 */
public class CreateCityDbIndex extends SpeedTestMonoThread {

  private static int                  cycleNumber;
  private int                         i = 0;
  private LineNumberReader            lineReader;

  private IndexWriter                 writer;
  static {

    try {

      ZipFile zipFile = new ZipFile("files/allCountries.zip");
      Enumeration<? extends ZipEntry> entries = zipFile.entries();

      while (entries.hasMoreElements()) {
        ZipEntry entry = entries.nextElement();
        if (entry.getName().equals("allCountries.txt")) {
          InputStream stream = zipFile.getInputStream(entry);
          LineNumberReader lnr = new LineNumberReader(new InputStreamReader(stream));
          int linenumber = 0;
          while (lnr.readLine() != null) {
            linenumber++;
          }
          lnr.close();
          cycleNumber = linenumber;
        }
      }

    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private SpatialContext              ctx;
  private RecursivePrefixTreeStrategy strategy;

  public CreateCityDbIndex() {
    super(cycleNumber);
  }

  @Override
  @Test(enabled = false)
  public void init() throws Exception {
    String buildDirectory = System.getProperty("buildDirectory", ".");
    if (buildDirectory == null)
      buildDirectory = ".";

    ZipFile zipFile = new ZipFile("files/allCountries.zip");
    Enumeration<? extends ZipEntry> entries = zipFile.entries();

    while (entries.hasMoreElements()) {

      ZipEntry entry = entries.nextElement();
      if (entry.getName().equals("allCountries.txt")) {

        InputStream stream = zipFile.getInputStream(entry);
        lineReader = new LineNumberReader(new InputStreamReader(stream));
      }
    }
    Directory dir = NIOFSDirectory.open(new File("Spatial"));
    IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_47, new StandardAnalyzer(Version.LUCENE_47));
    iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
    writer = new IndexWriter(dir, iwc);
    this.ctx = SpatialContext.GEO;
    SpatialPrefixTree grid = new GeohashPrefixTree(ctx, 11);
    this.strategy = new RecursivePrefixTreeStrategy(grid, "location");
  }

  @Override
  @Test(enabled = false)
  public void cycle() throws Exception {

    // String[] nextLine = reader.readNext();
    String readed = lineReader.readLine();
    String[] nextLine = readed.split("\\t");
    if (readed != null) {

      Double lat = ((Double) OType.convert(nextLine[4], Double.class)).doubleValue();
      Double lng = ((Double) OType.convert(nextLine[5], Double.class)).doubleValue();
      Shape shape = ctx.makePoint(lng, lat);
      try {
        writer.addDocument(newGeoDocument(i + "", shape));
      } catch (Exception e) {
        e.printStackTrace();
      }

      if (i % 1000 == 0) {
        writer.commit();
      }
      i++;
    }

  }

  private Document newGeoDocument(String rid, Shape shape) {

    Document doc = new Document();

    doc.add(new TextField("id", rid, Field.Store.YES));
    for (IndexableField f : strategy.createIndexableFields(shape)) {
      doc.add(f);
    }

    doc.add(new StoredField(strategy.getFieldName(), ctx.toString(shape)));

    return doc;
  }

  @Override
  @Test(enabled = false)
  public void deinit() throws Exception {

  }
}
