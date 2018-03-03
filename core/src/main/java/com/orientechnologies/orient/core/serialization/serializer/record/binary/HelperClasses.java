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
package com.orientechnologies.orient.core.serialization.serializer.record.binary;

import com.orientechnologies.orient.core.metadata.schema.OType;
import java.util.List;

/**
 *
 * @author mdjurovi
 */
public class HelperClasses {
  protected static class Tuple<T1, T2>{
    
    private final T1 firstVal;
    private final T2 secondVal;
    
    Tuple(T1 firstVal, T2 secondVal){
      this.firstVal = firstVal;
      this.secondVal = secondVal;
    }

    public T1 getFirstVal() {
      return firstVal;
    }

    public T2 getSecondVal() {
      return secondVal;
    }        
  }
  
  protected static class Triple<T1, T2, T3> extends Tuple<T1, T2>{
    
    private final T3 thirdVal;
    
    public Triple(T1 firstVal, T2 secondVal, T3 thirdVal) {
      super(firstVal, secondVal);
      this.thirdVal = thirdVal;
    }

    public T3 getThirdVal() {
      return thirdVal;
    }        
  }
  
  protected static class RecordInfo{
    List<Integer> fieldRelatedPositions;
    int fieldStartOffset;
    int fieldLength;
  }
  
  protected static class MapObjectData{
    int startPosition;
    int length;
    OType type;
    Object associatedKey;
  }
}
