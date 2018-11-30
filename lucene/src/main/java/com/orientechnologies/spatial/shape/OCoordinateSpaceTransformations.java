/*
 * Copyright 2018 OrientDB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.spatial.shape;

import com.mjt.geo.ostn02.EastingNorthing;
import com.mjt.geo.ostn02.LatitudeLongitude;

/**
 *
 * @author marko
 */
public class OCoordinateSpaceTransformations {
  //TODO review this numbers
  static final int WGS84SpaceRid = 4326;
  static final int BNGSpaceRid = 27700;
  
  
  static double[] transform(Integer fromSpaceRid, Integer toSpaceRid, double[] coordinate){
    if (fromSpaceRid == null || toSpaceRid == null || fromSpaceRid.equals(toSpaceRid)){
      return coordinate;
    }
    //TODO implement transformation
    if (fromSpaceRid == BNGSpaceRid && toSpaceRid == WGS84SpaceRid){
      EastingNorthing bngSpace = new EastingNorthing(coordinate[0], coordinate[1]);
      LatitudeLongitude wgsSpace = bngSpace.toLatitudeLongitude();
      double[] ret = {wgsSpace.getLat(), wgsSpace.getLon()};
      return ret;
    }
    if (fromSpaceRid == WGS84SpaceRid && toSpaceRid == BNGSpaceRid){
      LatitudeLongitude wgsSpace = new LatitudeLongitude(coordinate[0], coordinate[1]);
      EastingNorthing bngSpace = wgsSpace.toEastingNorthing();
      double[] ret = {bngSpace.getEast(), bngSpace.getNorth()};
      return ret;
    }
    return coordinate;
  }
  
}
