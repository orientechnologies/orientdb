/*
 * Copyright 2017 dominik.kopczynski.
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
package com.orientechnologies.orient.object.db.entity;

import java.util.ArrayList;
import java.util.HashMap;

/** @author dominik.kopczynski */
public class NestedContainer {
  private String name;
  HashMap<String, ArrayList<NestedContent>> foo = new HashMap<String, ArrayList<NestedContent>>();

  public NestedContainer() {}

  public void setFoo(HashMap<String, ArrayList<NestedContent>> foo) {
    this.foo = foo;
  }

  public HashMap<String, ArrayList<NestedContent>> getFoo() {
    return foo;
  }

  public NestedContainer(String name) {
    this.name = name;
    ArrayList<NestedContent> al = new ArrayList<NestedContent>();
    NestedContent dd1 = new NestedContent("Jack / " + name);
    NestedContent dd2 = new NestedContent("Maria / " + name);
    NestedContent dd3 = new NestedContent("Micheal / " + name);
    al.add(dd1);
    al.add(dd2);
    al.add(dd3);

    foo.put("key-1", al);
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void info() {
    System.out.println("Name: " + getName());
    for (String key : getFoo().keySet()) {
      for (NestedContent cc2 : getFoo().get(key)) {
        System.out.println(key + " " + cc2.getName());
      }
    }
    System.out.println("============================");
  }
}
