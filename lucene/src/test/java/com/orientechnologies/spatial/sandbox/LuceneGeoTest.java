/**
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * <p>For more information: http://www.orientdb.com
 */
package com.orientechnologies.spatial.sandbox;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.spatial.shape.OMultiPolygonShapeBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.util.Map;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.apache.lucene.store.RAMDirectory;
import org.junit.Assert;
import org.junit.Test;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.SpatialRelation;

/** Created by Enrico Risa on 01/10/15. */
public class LuceneGeoTest {

  @Test
  public void geoIntersectTest() throws IOException, ParseException {

    RecursivePrefixTreeStrategy strategy =
        new RecursivePrefixTreeStrategy(
            new GeohashPrefixTree(JtsSpatialContext.GEO, 11), "location");

    strategy.setDistErrPct(0);

    IndexWriterConfig conf = new IndexWriterConfig(new StandardAnalyzer());
    final RAMDirectory directory = new RAMDirectory();
    final IndexWriter writer = new IndexWriter(directory, conf);

    Shape point = JtsSpatialContext.GEO.getWktShapeParser().parse("POINT (9.4714708 47.6819432)");

    Shape polygon =
        JtsSpatialContext.GEO
            .getWktShapeParser()
            .parse(
                "POLYGON((9.481201171875 47.64885294675266,9.471416473388672 47.65128140482982,9.462661743164062 47.64781214443791,9.449443817138672 47.656947367880335,9.445838928222656 47.66110972448931,9.455795288085938 47.667352637215,9.469013214111328 47.67255449415724,9.477081298828125 47.679142768657066,9.490299224853516 47.678680460743834,9.506263732910156 47.679258344995326,9.51364517211914 47.68191653011071,9.518795013427734 47.677177931734406,9.526691436767578 47.679489496903706,9.53390121459961 47.67139857075435,9.50918197631836 47.66180341832901,9.50815200805664 47.6529003141482,9.51192855834961 47.64654002455372,9.504375457763672 47.64237650648966,9.49270248413086 47.649662445325035,9.48617935180664 47.65151268066222,9.481201171875 47.64885294675266))");

    Document doc = new Document();

    Assert.assertNotEquals(point.relate(polygon), SpatialRelation.INTERSECTS);
    for (IndexableField f : strategy.createIndexableFields(point)) {
      doc.add(f);
    }

    writer.addDocument(doc);
    writer.commit();

    SpatialArgs args = new SpatialArgs(SpatialOperation.Intersects, polygon.getBoundingBox());
    Query filterQuery = strategy.makeQuery(args);
    IndexReader reader = DirectoryReader.open(directory);

    IndexSearcher searcher = new IndexSearcher(reader);

    BooleanQuery q =
        new BooleanQuery.Builder()
            .add(filterQuery, BooleanClause.Occur.MUST)
            .add(new MatchAllDocsQuery(), BooleanClause.Occur.SHOULD)
            .build();

    TopDocs search = searcher.search(q, 1000);
    Assert.assertEquals(search.totalHits, 0);

    reader.close();
    writer.close();
  }

  @Test
  public void geoSpeedTest() throws IOException, ParseException {

    RecursivePrefixTreeStrategy strategy =
        new RecursivePrefixTreeStrategy(
            new GeohashPrefixTree(JtsSpatialContext.GEO, 11), "location");

    IndexWriterConfig conf = new IndexWriterConfig(new StandardAnalyzer());
    final RAMDirectory directory = new RAMDirectory();
    final IndexWriter writer = new IndexWriter(directory, conf);

    Shape multiPolygon =
        JtsSpatialContext.GEO
            .getWktShapeParser()
            .parse(
                "MULTIPOLYGON (((15.520376 38.231155, 15.160243 37.444046, 15.309898 37.134219, 15.099988 36.619987, 14.335229 36.996631, 13.826733 37.104531, 12.431004 37.61295, 12.570944 38.126381, 13.741156 38.034966, 14.761249 38.143874, 15.520376 38.231155)), ((9.210012 41.209991, 9.809975 40.500009, 9.669519 39.177376, 9.214818 39.240473, 8.806936 38.906618, 8.428302 39.171847, 8.388253 40.378311, 8.159998 40.950007, 8.709991 40.899984, 9.210012 41.209991)), ((12.376485 46.767559, 13.806475 46.509306, 13.69811 46.016778, 13.93763 45.591016, 13.141606 45.736692, 12.328581 45.381778, 12.383875 44.885374, 12.261453 44.600482, 12.589237 44.091366, 13.526906 43.587727, 14.029821 42.761008, 15.14257 41.95514, 15.926191 41.961315, 16.169897 41.740295, 15.889346 41.541082, 16.785002 41.179606, 17.519169 40.877143, 18.376687 40.355625, 18.480247 40.168866, 18.293385 39.810774, 17.73838 40.277671, 16.869596 40.442235, 16.448743 39.795401, 17.17149 39.4247, 17.052841 38.902871, 16.635088 38.843572, 16.100961 37.985899, 15.684087 37.908849, 15.687963 38.214593, 15.891981 38.750942, 16.109332 38.964547, 15.718814 39.544072, 15.413613 40.048357, 14.998496 40.172949, 14.703268 40.60455, 14.060672 40.786348, 13.627985 41.188287, 12.888082 41.25309, 12.106683 41.704535, 11.191906 42.355425, 10.511948 42.931463, 10.200029 43.920007, 9.702488 44.036279, 8.888946 44.366336, 8.428561 44.231228, 7.850767 43.767148, 7.435185 43.693845, 7.549596 44.127901, 7.007562 44.254767, 6.749955 45.028518, 7.096652 45.333099, 6.802355 45.70858, 6.843593 45.991147, 7.273851 45.776948, 7.755992 45.82449, 8.31663 46.163642, 8.489952 46.005151, 8.966306 46.036932, 9.182882 46.440215, 9.922837 46.314899, 10.363378 46.483571, 10.442701 46.893546, 11.048556 46.751359, 11.164828 46.941579, 12.153088 47.115393, 12.376485 46.767559)))");

    Document doc = new Document();

    for (IndexableField f : strategy.createIndexableFields(multiPolygon)) {
      doc.add(f);
    }

    writer.addDocument(doc);
    writer.commit();

    writer.close();
  }

  @Test
  public void geoSpeedTestInternal() throws IOException, ParseException {

    RecursivePrefixTreeStrategy strategy =
        new RecursivePrefixTreeStrategy(
            new GeohashPrefixTree(JtsSpatialContext.GEO, 11), "location");

    IndexWriterConfig conf = new IndexWriterConfig(new StandardAnalyzer());
    final RAMDirectory directory = new RAMDirectory();
    final IndexWriter writer = new IndexWriter(directory, conf);

    ODocument entries = loadMultiPolygon();

    OMultiPolygonShapeBuilder builder = new OMultiPolygonShapeBuilder();

    Shape multiPolygon = builder.fromDoc(entries);

    Document doc = new Document();

    for (IndexableField f : strategy.createIndexableFields(multiPolygon)) {
      doc.add(f);
    }

    writer.addDocument(doc);
    writer.commit();

    writer.close();
  }

  protected ODocument loadMultiPolygon() {

    try {
      InputStream systemResourceAsStream = ClassLoader.getSystemResourceAsStream("italy.json");

      ODocument doc = new ODocument().fromJSON(systemResourceAsStream);

      Map geometry = doc.field("geometry");

      String type = (String) geometry.get("type");
      ODocument location = new ODocument("O" + type);
      location.field("coordinates", geometry.get("coordinates"));
      return location;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }
}
