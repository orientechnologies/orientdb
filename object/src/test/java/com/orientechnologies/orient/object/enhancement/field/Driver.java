/*
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

package com.orientechnologies.orient.object.enhancement.field;

import javax.persistence.Basic;
import javax.persistence.Id;
import javax.persistence.Lob;

/**
 * Dummy entity class created for test purposes.
 * 
 * @author diegomtassis <a href="mailto:dta@compart.com">Diego Martin Tassis</a>
 */
public class Driver {

  @Id
  private String id;

  @Basic
  private String name;

  @Basic
  @Lob
  private byte[] imageData;

  public String getId() {
    return this.id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getName() {
    return this.name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public byte[] getImageData() {
    return this.imageData;
  }

  public void setImageData(byte[] imageData) {
    this.imageData = imageData;
  }
}