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

package com.orientechnologies.lucene.shape;

import com.spatial4j.core.shape.Shape;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.LineString;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by Enrico Risa on 13/08/15.
 */
public abstract class OComplexShapeBuilder<T extends Shape> extends OShapeBuilder<T> {






  protected List<List<Double>> coordinatesFromLineString(LineString ring) {

    Coordinate[] coordinates = ring.getCoordinates();
    List<List<Double>> numbers = new ArrayList<List<Double>>();
    for (Coordinate coordinate : coordinates) {

      numbers.add(Arrays.asList(coordinate.x, coordinate.y));
    }
    return numbers;
  }
}
