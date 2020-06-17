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
package com.orientechnologies.spatial.functions;

import static org.assertj.core.api.Assertions.assertThat;

import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.spatial.BaseSpatialLuceneTest;
import com.orientechnologies.spatial.shape.OShapeFactory;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.jts.JtsGeometry;

/** Created by Enrico Risa on 28/09/15. */
public class LuceneSpatialMiscFunctionsTest extends BaseSpatialLuceneTest {

  @Test
  public void testStEquals() {

    List<ODocument> execute =
        db.command(
                new OCommandSQL(
                    "SELECT ST_Equals(ST_GeomFromText('LINESTRING(0 0, 10 10)'), ST_GeomFromText('LINESTRING(0 0, 5 5, 10 10)'))"))
            .execute();
    ODocument next = execute.iterator().next();
    Assert.assertEquals(next.field("ST_Equals"), true);
  }

  @Test
  public void testStEqualsPoint() {

    List<ODocument> execute =
        db.command(
                new OCommandSQL(
                    "select ST_Equals(ST_GeomFromText('POINT (55.78639 37.58378)'), ST_GeomFromText('POINT (55.78639 37.58378)'))"))
            .execute();
    ODocument next = execute.iterator().next();
    Assert.assertEquals(next.field("ST_Equals"), true);
  }

  @Test
  public void testStWithinPoint() {

    List<ODocument> execute =
        db.command(
                new OCommandSQL(
                    "select ST_Within(ST_GeomFromText('POINT (55.78639 37.58378)'), ST_GeomFromText('POINT (55.78639 37.58378)'))"))
            .execute();
    ODocument next = execute.iterator().next();
    Assert.assertEquals(true, next.field("ST_Within"));
  }

  @Test
  public void testStContainsPoint() {

    List<ODocument> execute =
        db.command(
                new OCommandSQL(
                    "select ST_Contains(ST_GeomFromText('POINT (55.78639 37.58378)'), ST_GeomFromText('POINT (55.78639 37.58378)'))"))
            .execute();
    ODocument next = execute.iterator().next();
    Assert.assertEquals(true, next.field("ST_Contains"));
  }

  // TODO reanable and check byte[]
  @Test
  @Ignore
  public void testAsBinary() {

    List<ODocument> execute =
        db.command(new OCommandSQL("SELECT ST_AsBinary(ST_GeomFromText('LINESTRING(0 0, 10 10)'))"))
            .execute();
    ODocument next = execute.iterator().next();
    // TODO CHANGE
    Assert.assertNull(next.field("ST_AsBinary"));
  }

  @Test
  public void testEnvelope() {

    List<ODocument> execute =
        db.command(new OCommandSQL("SELECT ST_AsText(ST_Envelope('LINESTRING(0 0, 1 3)'))"))
            .execute();
    ODocument next = execute.iterator().next();
    Assert.assertEquals(next.field("ST_AsText"), "POLYGON ((0 0, 0 3, 1 3, 1 0, 0 0))");
  }

  @Test
  public void testBuffer() {

    List<ODocument> execute =
        db.command(
                new OCommandSQL("SELECT ST_Buffer(ST_GeomFromText('POINT(100 90)'),50) as buffer;"))
            .execute();
    ODocument next = execute.iterator().next();

    OElement buffer = next.getProperty("buffer");
    List coordinates = buffer.getProperty("coordinates");
    Assert.assertNotNull(coordinates);
    Assert.assertEquals(1, coordinates.size());

    List<List<Double>> arrays = buildArrays1();

    List<List<Double>> exp = (List<List<Double>>) coordinates.get(0);
    for (int i = 0; i < arrays.size(); i++) {
      List<Double> expected = arrays.get(i);
      List<Double> actual = (List) exp.get(i);
      Assert.assertEquals(expected.get(0), actual.get(0), 0.00000001d);
    }

    //    Assert.assertEquals(next.field("ST_AsText"),
    //        "POLYGON ((150 90, 149.0392640201615 80.24548389919359, 146.19397662556435
    // 70.86582838174552, 141.57348061512727 62.22148834901989, 135.35533905932738
    // 54.64466094067263, 127.77851165098011 48.42651938487274, 119.1341716182545 43.80602337443566,
    // 109.75451610080641 40.960735979838475, 100 40, 90.24548389919359 40.960735979838475,
    // 80.86582838174552 43.80602337443566, 72.2214883490199 48.426519384872734, 64.64466094067262
    // 54.64466094067262, 58.426519384872734 62.22148834901989, 53.80602337443566 70.86582838174553,
    // 50.960735979838475 80.24548389919362, 50 90.00000000000004, 50.96073597983849
    // 99.75451610080646, 53.80602337443568 109.13417161825454, 58.426519384872776
    // 117.77851165098016, 64.64466094067268 125.35533905932743, 72.22148834901996
    // 131.57348061512732, 80.8658283817456 136.19397662556437, 90.2454838991937 139.03926402016154,
    // 100.00000000000013 140, 109.75451610080654 139.0392640201615, 119.13417161825463
    // 136.1939766255643, 127.77851165098025 131.57348061512718, 135.3553390593275
    // 125.35533905932726, 141.57348061512735 117.77851165097996, 146.1939766255644
    // 109.13417161825431, 149.03926402016157 99.75451610080621, 150 90))");

    execute =
        db.command(
                new OCommandSQL(
                    "SELECT ST_Buffer(ST_GeomFromText('POINT(100 90)'), 50, { quadSegs : 2 }) as buffer;"))
            .execute();
    next = execute.iterator().next();

    //    Assert.assertEquals(next.field("ST_AsText"),
    //        "POLYGON ((150 90, 135.35533905932738 54.64466094067263, 100 40, 64.64466094067262
    // 54.64466094067262, 50 90, 64.64466094067262 125.35533905932738, 99.99999999999999 140,
    // 135.35533905932738 125.35533905932738, 150 90))");

    buffer = next.getProperty("buffer");
    coordinates = buffer.getProperty("coordinates");
    Assert.assertNotNull(coordinates);
    Assert.assertEquals(1, coordinates.size());
    arrays = buildArrays2();
    exp = (List<List<Double>>) coordinates.get(0);
    for (int i = 0; i < arrays.size(); i++) {
      List<Double> expected = arrays.get(i);
      List<Double> actual = exp.get(i);
      Assert.assertEquals(expected.get(0), actual.get(0), 0.00000001d);
    }

    execute =
        db.command(
                new OCommandSQL(
                    "SELECT ST_Buffer(ST_GeomFromText('LINESTRING(0 0,75 75,75 0)'), 10, { 'endCap' : 'square' }) as buffer;"))
            .execute();
    next = execute.iterator().next();
    //    Assert.assertEquals(next.field("ST_AsText"),
    //        "POLYGON ((67.92893218813452 82.07106781186548, 69.44429766980397 83.31469612302546,
    // 71.1731656763491 84.23879532511287, 73.04909677983872 84.80785280403231, 75 85,
    // 76.95090322016128 84.80785280403231, 78.8268343236509 84.23879532511287, 80.55570233019603
    // 83.31469612302546, 82.07106781186548 82.07106781186548, 83.31469612302546 80.55570233019603,
    // 84.23879532511287 78.8268343236509, 84.80785280403231 76.95090322016128, 85 75, 85 0,
    // 84.80785280403231 -1.9509032201612824, 84.23879532511287 -3.826834323650898,
    // 83.31469612302546 -5.555702330196022, 82.07106781186548 -7.071067811865475, 80.55570233019603
    // -8.314696123025453, 78.8268343236509 -9.238795325112868, 76.95090322016128
    // -9.807852804032304, 75 -10, 73.04909677983872 -9.807852804032304, 71.1731656763491
    // -9.238795325112868, 69.44429766980397 -8.314696123025453, 67.92893218813452
    // -7.0710678118654755, 66.68530387697454 -5.555702330196022, 65.76120467488713
    // -3.8268343236508944, 65.19214719596769 -1.9509032201612773, 65 0, 65 50.85786437626905,
    // 7.071067811865475 -7.071067811865475, 5.555702330196023 -8.314696123025453,
    // 3.8268343236508984 -9.238795325112868, 1.9509032201612833 -9.807852804032304,
    // 0.0000000000000006 -10, -1.950903220161282 -9.807852804032304, -3.826834323650897
    // -9.238795325112868, -5.55570233019602 -8.314696123025453, -7.071067811865475
    // -7.0710678118654755, -8.314696123025453 -5.555702330196022, -9.238795325112868
    // -3.826834323650899, -9.807852804032304 -1.9509032201612861, -10 -0.0000000000000012,
    // -9.807852804032304 1.9509032201612837, -9.238795325112866 3.8268343236509006,
    // -8.314696123025449 5.555702330196026, -7.071067811865475 7.071067811865475, 67.92893218813452
    // 82.07106781186548))");

    buffer = next.getProperty("buffer");
    coordinates = buffer.getProperty("coordinates");
    Assert.assertNotNull(coordinates);
    Assert.assertEquals(1, coordinates.size());
    arrays = buildArrays3();
    exp = (List<List<Double>>) coordinates.get(0);
    for (int i = 0; i < arrays.size(); i++) {
      List<Double> expected = arrays.get(i);
      List<Double> actual = exp.get(i);
      Assert.assertEquals(expected.get(0), actual.get(0), 0.00000001d);
    }
  }

  private List<List<Double>> buildArrays1() {
    List<List<Double>> list = new ArrayList<>();
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(150D);
    ((List) list.get(list.size() - 1)).add(90D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(149.0392640201615D);
    ((List) list.get(list.size() - 1)).add(80.24548389919359D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(146.19397662556435D);
    ((List) list.get(list.size() - 1)).add(70.86582838174552D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(141.57348061512727D);
    ((List) list.get(list.size() - 1)).add(62.22148834901989D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(135.35533905932738D);
    ((List) list.get(list.size() - 1)).add(54.64466094067263D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(127.77851165098011D);
    ((List) list.get(list.size() - 1)).add(48.42651938487274D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(119.1341716182545D);
    ((List) list.get(list.size() - 1)).add(43.80602337443566D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(109.75451610080641D);
    ((List) list.get(list.size() - 1)).add(40.960735979838475D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(100D);
    ((List) list.get(list.size() - 1)).add(40D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(90.24548389919359D);
    ((List) list.get(list.size() - 1)).add(40.960735979838475D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(80.86582838174552D);
    ((List) list.get(list.size() - 1)).add(43.80602337443566D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(72.2214883490199D);
    ((List) list.get(list.size() - 1)).add(48.426519384872734D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(64.64466094067262D);
    ((List) list.get(list.size() - 1)).add(54.64466094067262D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(58.426519384872734D);
    ((List) list.get(list.size() - 1)).add(62.22148834901989D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(53.80602337443566D);
    ((List) list.get(list.size() - 1)).add(70.86582838174553D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(50.960735979838475D);
    ((List) list.get(list.size() - 1)).add(80.24548389919362D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(50D);
    ((List) list.get(list.size() - 1)).add(90.00000000000004D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(50.96073597983849D);
    ((List) list.get(list.size() - 1)).add(99.75451610080646D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(53.80602337443568D);
    ((List) list.get(list.size() - 1)).add(109.13417161825454D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(58.426519384872776D);
    ((List) list.get(list.size() - 1)).add(117.77851165098016D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(64.64466094067268D);
    ((List) list.get(list.size() - 1)).add(125.35533905932743D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(72.22148834901996D);
    ((List) list.get(list.size() - 1)).add(131.57348061512732D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(80.8658283817456D);
    ((List) list.get(list.size() - 1)).add(136.19397662556437D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(90.2454838991937D);
    ((List) list.get(list.size() - 1)).add(139.03926402016154D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(100.00000000000013D);
    ((List) list.get(list.size() - 1)).add(140D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(109.75451610080654D);
    ((List) list.get(list.size() - 1)).add(139.0392640201615D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(119.13417161825463D);
    ((List) list.get(list.size() - 1)).add(136.1939766255643D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(127.77851165098025D);
    ((List) list.get(list.size() - 1)).add(131.57348061512718D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(135.3553390593275D);
    ((List) list.get(list.size() - 1)).add(125.35533905932726D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(141.57348061512735D);
    ((List) list.get(list.size() - 1)).add(117.77851165097996D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(146.1939766255644D);
    ((List) list.get(list.size() - 1)).add(109.13417161825431D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(149.03926402016157D);
    ((List) list.get(list.size() - 1)).add(99.75451610080621D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(150D);
    ((List) list.get(list.size() - 1)).add(90D);

    return list;
  }

  private List<List<Double>> buildArrays2() {
    List<List<Double>> list = new ArrayList<>();

    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(150D);
    ((List) list.get(list.size() - 1)).add(90D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(135.35533905932738D);
    ((List) list.get(list.size() - 1)).add(54.64466094067263D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(100D);
    ((List) list.get(list.size() - 1)).add(40D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(64.64466094067262D);
    ((List) list.get(list.size() - 1)).add(54.64466094067262D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(50D);
    ((List) list.get(list.size() - 1)).add(90D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(64.64466094067262D);
    ((List) list.get(list.size() - 1)).add(125.35533905932738D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(99.99999999999999D);
    ((List) list.get(list.size() - 1)).add(140D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(135.35533905932738D);
    ((List) list.get(list.size() - 1)).add(125.35533905932738D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(150D);
    ((List) list.get(list.size() - 1)).add(90D);

    return list;
  }

  private List<List<Double>> buildArrays3() {
    List<List<Double>> list = new ArrayList<>();

    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(67.92893218813452D);
    ((List) list.get(list.size() - 1)).add(82.07106781186548D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(69.44429766980397D);
    ((List) list.get(list.size() - 1)).add(83.31469612302546D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(71.1731656763491D);
    ((List) list.get(list.size() - 1)).add(84.23879532511287D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(73.04909677983872D);
    ((List) list.get(list.size() - 1)).add(84.80785280403231D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(75D);
    ((List) list.get(list.size() - 1)).add(85D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(76.95090322016128D);
    ((List) list.get(list.size() - 1)).add(84.80785280403231D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(78.8268343236509D);
    ((List) list.get(list.size() - 1)).add(84.23879532511287D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(80.55570233019603D);
    ((List) list.get(list.size() - 1)).add(83.31469612302546D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(82.07106781186548D);
    ((List) list.get(list.size() - 1)).add(82.07106781186548D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(83.31469612302546D);
    ((List) list.get(list.size() - 1)).add(80.55570233019603D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(84.23879532511287D);
    ((List) list.get(list.size() - 1)).add(78.8268343236509D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(84.80785280403231D);
    ((List) list.get(list.size() - 1)).add(76.95090322016128D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(85D);
    ((List) list.get(list.size() - 1)).add(75D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(85D);
    ((List) list.get(list.size() - 1)).add(0D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(84.80785280403231D);
    ((List) list.get(list.size() - 1)).add(-1.9509032201612824D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(84.23879532511287D);
    ((List) list.get(list.size() - 1)).add(-3.826834323650898D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(83.31469612302546D);
    ((List) list.get(list.size() - 1)).add(-5.555702330196022D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(82.07106781186548D);
    ((List) list.get(list.size() - 1)).add(-7.071067811865475D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(80.55570233019603D);
    ((List) list.get(list.size() - 1)).add(-8.314696123025453D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(78.8268343236509D);
    ((List) list.get(list.size() - 1)).add(-9.238795325112868D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(76.95090322016128D);
    ((List) list.get(list.size() - 1)).add(-9.807852804032304D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(75D);
    ((List) list.get(list.size() - 1)).add(-10D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(73.04909677983872D);
    ((List) list.get(list.size() - 1)).add(-9.807852804032304D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(71.1731656763491D);
    ((List) list.get(list.size() - 1)).add(-9.238795325112868D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(69.44429766980397D);
    ((List) list.get(list.size() - 1)).add(-8.314696123025453D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(67.92893218813452D);
    ((List) list.get(list.size() - 1)).add(-7.0710678118654755D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(66.68530387697454D);
    ((List) list.get(list.size() - 1)).add(-5.555702330196022D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(65.76120467488713D);
    ((List) list.get(list.size() - 1)).add(-3.8268343236508944D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(65.19214719596769D);
    ((List) list.get(list.size() - 1)).add(-1.9509032201612773D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(65D);
    ((List) list.get(list.size() - 1)).add(0D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(65D);
    ((List) list.get(list.size() - 1)).add(50.85786437626905D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(7.071067811865475D);
    ((List) list.get(list.size() - 1)).add(-7.071067811865475D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(5.555702330196023D);
    ((List) list.get(list.size() - 1)).add(-8.314696123025453D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(3.8268343236508984D);
    ((List) list.get(list.size() - 1)).add(-9.238795325112868D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(1.9509032201612833D);
    ((List) list.get(list.size() - 1)).add(-9.807852804032304D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(0.0000000000000006D);
    ((List) list.get(list.size() - 1)).add(-10D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(-1.950903220161282D);
    ((List) list.get(list.size() - 1)).add(-9.807852804032304D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(-3.826834323650897D);
    ((List) list.get(list.size() - 1)).add(-9.238795325112868D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(-5.55570233019602D);
    ((List) list.get(list.size() - 1)).add(-8.314696123025453D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(-7.071067811865475D);
    ((List) list.get(list.size() - 1)).add(-7.0710678118654755D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(-8.314696123025453D);
    ((List) list.get(list.size() - 1)).add(-5.555702330196022D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(-9.238795325112868D);
    ((List) list.get(list.size() - 1)).add(-3.826834323650899D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(-9.807852804032304D);
    ((List) list.get(list.size() - 1)).add(-1.9509032201612861D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(-10D);
    ((List) list.get(list.size() - 1)).add(-0.0000000000000012D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(-9.807852804032304D);
    ((List) list.get(list.size() - 1)).add(1.9509032201612837D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(-9.238795325112866D);
    ((List) list.get(list.size() - 1)).add(3.8268343236509006D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(-8.314696123025449D);
    ((List) list.get(list.size() - 1)).add(5.555702330196026D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(-7.071067811865475D);
    ((List) list.get(list.size() - 1)).add(7.071067811865475D);
    list.add(new ArrayList());
    ((List) list.get(list.size() - 1)).add(67.92893218813452D);
    ((List) list.get(list.size() - 1)).add(82.07106781186548D);

    return list;
  }

  // todo check distance
  @Test
  public void testDistance() {

    List<ODocument> execute =
        db.command(
                new OCommandSQL(
                    "SELECT ST_Distance(ST_GeomFromText('POINT(-72.1235 42.3521)'),ST_GeomFromText('LINESTRING(-72.1260 42.45, -72.123 42.1546)'))"))
            .execute();
    ODocument next = execute.iterator().next();

    //      Assert.assertEquals(next.field("ST_Distance"), 0.0015056772638228177);
    assertThat(next.<Double>field("ST_Distance")).isEqualTo(0.0015056772638228177);

    execute =
        db.command(
                new OCommandSQL(
                    "SELECT  ST_Distance( ST_GeomFromText('LINESTRING(13.45 52.47,13.46 52.48)'), ST_GeomFromText('LINESTRING(13.00 52.00,13.1 52.2)'))"))
            .execute();
    next = execute.iterator().next();

    //      Assert.assertEquals(next.field("ST_Distance"), 0.44204072210600415);

    assertThat(next.<Double>field("ST_Distance")).isEqualTo(0.44204072210600415);
  }

  @Test
  public void testDisjoint() {

    List<ODocument> execute =
        db.command(new OCommandSQL("SELECT ST_Disjoint('POINT(0 0)', 'LINESTRING ( 2 0, 0 2 )');"))
            .execute();
    ODocument next = execute.iterator().next();

    Assert.assertEquals(next.field("ST_Disjoint"), true);

    execute =
        db.command(new OCommandSQL("SELECT ST_Disjoint('POINT(0 0)', 'LINESTRING ( 0 0, 0 2 )');"))
            .execute();
    next = execute.iterator().next();

    Assert.assertEquals(next.field("ST_Disjoint"), false);
  }

  @Test
  public void testWktPolygon() throws ParseException {

    Shape shape = OShapeFactory.INSTANCE.fromObject("POLYGON((0 0, 10 0, 10 5, 0 5, 0 0))");

    Assert.assertEquals(shape instanceof JtsGeometry, true);

    JtsGeometry geom = (JtsGeometry) shape;
    Assert.assertEquals(geom.getGeom() instanceof Polygon, true);
  }
}
