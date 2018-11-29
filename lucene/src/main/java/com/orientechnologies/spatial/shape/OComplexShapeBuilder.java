/**
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * <p>
 * For more information: http://www.orientdb.com
 */
package com.orientechnologies.spatial.shape;

import org.locationtech.jts.geom.*;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Rectangle;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.ShapeCollection;
import org.locationtech.spatial4j.shape.jts.JtsGeometry;
import org.locationtech.spatial4j.shape.jts.JtsPoint;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.orientechnologies.spatial.shape.CoordinateSpaceTransformations.WGS84SpaceRid;

/**
 * Created by Enrico Risa on 13/08/15.
 */
public abstract class OComplexShapeBuilder<T extends Shape> extends OShapeBuilder<T> {

  protected List<List<Double>> coordinatesFromLineString(LineString ring, Integer srid) {

    Coordinate[] coordinates = ring.getCoordinates();
    List<List<Double>> numbers = new ArrayList<List<Double>>();
    for (Coordinate coordinate : coordinates) {
      double[] coord = {coordinate.x, coordinate.y};
      coord = CoordinateSpaceTransformations.transform(srid, WGS84SpaceRid, coord);
      numbers.add(Arrays.asList(coord[0], coord[1]));
    }
    return numbers;
  }

  protected LineString createLineString(List<List<Number>> coordinates, Integer srid) {
    Coordinate[] coords = new Coordinate[coordinates.size()];
    int i = 0;
    for (List<Number> c : coordinates) {
      double[] coord = {c.get(0).doubleValue(), c.get(1).doubleValue()};
      //this is used inf fromDoc, so reversed tranformation
      coord = CoordinateSpaceTransformations.transform(WGS84SpaceRid, srid, coord);
      coords[i] = new Coordinate(coord[0], coord[1]);
      i++;
    }
    return GEOMETRY_FACTORY.createLineString(coords);
  }

  protected JtsGeometry createMultiPoint(ShapeCollection<JtsPoint> geometries, Integer srid) {

    Coordinate[] points = new Coordinate[geometries.size()];

    int i = 0;

    for (JtsPoint geometry : geometries) {
      double[] coord = {geometry.getX(), geometry.getY()};
      coord = CoordinateSpaceTransformations.transform(srid, WGS84SpaceRid, coord);
      points[i] = new Coordinate(coord[0], coord[1]);
      i++;
    }

    MultiPoint multiPoints = GEOMETRY_FACTORY.createMultiPoint(points);

    return SPATIAL_CONTEXT.makeShape(multiPoints);
  }

  protected JtsGeometry createMultiLine(ShapeCollection<JtsGeometry> geometries, Integer srid) {

    LineString[] multiLineString = new LineString[geometries.size()];

    int i = 0;

    for (JtsGeometry geometry : geometries) {
      multiLineString[i] = (LineString) geometry.getGeom();
      Coordinate[] coordinates = multiLineString[i].getCoordinates();
      Coordinate[] newCoordinates = new Coordinate[coordinates.length];
      for (int j = 0; j < coordinates.length; j++){
        Coordinate coordinate = coordinates[j];
        double[] coord = {coordinate.x, coordinate.y};
        coord = CoordinateSpaceTransformations.transform(srid, WGS84SpaceRid, coord);
        newCoordinates[j] = new Coordinate(coord[0], coord[1]);
      }
      multiLineString[i] = GEOMETRY_FACTORY.createLineString(newCoordinates);
      i++;
    }

    MultiLineString multiPoints = GEOMETRY_FACTORY.createMultiLineString(multiLineString);

    return SPATIAL_CONTEXT.makeShape(multiPoints);
  }

  protected JtsGeometry createMultiPolygon(ShapeCollection<Shape> geometries, Integer srid) {

    Polygon[] polygons = new Polygon[geometries.size()];

    int i = 0;

    for (Shape geometry : geometries) {
      if (geometry instanceof JtsGeometry) {
        polygons[i] = (Polygon) ((JtsGeometry) geometry).getGeom();        
      } else {
        Rectangle rectangle = (Rectangle) geometry;
        Geometry geometryFrom = SPATIAL_CONTEXT.getGeometryFrom(rectangle);
        polygons[i] = (Polygon) geometryFrom;
      }

      Coordinate[] coordinates = polygons[i].getCoordinates();
      Coordinate[] newCoordinates = new Coordinate[coordinates.length];
      for (int j = 0; j < coordinates.length; j++){
        Coordinate coordinate = coordinates[j];
        double[] coord = {coordinate.x, coordinate.y};
        coord = CoordinateSpaceTransformations.transform(srid, WGS84SpaceRid, coord);
        newCoordinates[j] = new Coordinate(coord[0], coord[1]);
      }
      polygons[i] = GEOMETRY_FACTORY.createPolygon(newCoordinates);
      i++;
    }

    MultiPolygon multiPolygon = GEOMETRY_FACTORY.createMultiPolygon(polygons);

    return SPATIAL_CONTEXT.makeShape(multiPolygon);
  }

  protected boolean isMultiPolygon(ShapeCollection<Shape> collection) {

    boolean isMultiPolygon = true;
    for (Shape shape : collection) {

      if (!isPolygon(shape)) {
        isMultiPolygon = false;
        break;
      }
    }
    return isMultiPolygon;
  }

  protected boolean isMultiPoint(ShapeCollection<Shape> collection) {

    boolean isMultipoint = true;
    for (Shape shape : collection) {

      if (!isPoint(shape)) {
        isMultipoint = false;
        break;
      }
    }
    return isMultipoint;
  }

  protected boolean isMultiLine(ShapeCollection<Shape> collection) {

    boolean isMultipoint = true;
    for (Shape shape : collection) {

      if (!isLineString(shape)) {
        isMultipoint = false;
        break;
      }
    }
    return isMultipoint;
  }

  private boolean isLineString(Shape shape) {
    if (shape instanceof JtsGeometry) {
      Geometry geom = ((JtsGeometry) shape).getGeom();
      return geom instanceof LineString;
    }
    return false;
  }

  protected boolean isPoint(Shape shape) {
    return shape instanceof Point;
  }

  protected boolean isPolygon(Shape shape) {

    if (shape instanceof JtsGeometry) {
      Geometry geom = ((JtsGeometry) shape).getGeom();
      return geom instanceof Polygon;
    }
    if (shape instanceof Rectangle) {
      return true;
    }
    return false;
  }
}
