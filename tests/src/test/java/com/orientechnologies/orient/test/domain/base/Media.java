/*
 *
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
package com.orientechnologies.orient.test.domain.base;

import com.orientechnologies.orient.core.record.impl.OBlob;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import javax.persistence.Id;
import javax.persistence.OneToOne;
import javax.persistence.Version;

/** @author Luca Molino (molino.luca--at--gmail.com) */
public class Media {

  @Id private Object id;

  @Version private Object version;

  private String name;

  @OneToOne(orphanRemoval = true)
  private OBlob content;

  public Object getId() {
    return id;
  }

  public void setId(Object id) {
    this.id = id;
  }

  public Object getVersion() {
    return version;
  }

  public void setVersion(Object version) {
    this.version = version;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public OBlob getContent() {
    return content;
  }

  public void setContent(OBlob content) {
    OBlob current = this.getContent();
    this.content = content;
    if (current != null) current.getRecord().delete();
  }

  public void setContent(byte[] bytes) {
    this.content = new ORecordBytes(bytes);
  }
}
