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

/**
 *
 * @author marko
 */
public class CoordinateSpaceTransformations {
  //TODO review this numbers
  static final int WGS84SpaceRid = 4326;
  static final int BNGSpaceRid = 2;
  
  
  static double[] transform(Integer fromSpaceRid, Integer toSpaceRid, double[] coordinate){
    if (fromSpaceRid == null || toSpaceRid == null || fromSpaceRid.equals(toSpaceRid)){
      return coordinate;
    }
    //TODO implement transformation
    return coordinate;
  }
  
}
