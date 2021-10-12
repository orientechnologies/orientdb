/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.server.config;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import jakarta.xml.bind.JAXBContext;
import jakarta.xml.bind.JAXBException;
import jakarta.xml.bind.Marshaller;
import jakarta.xml.bind.Unmarshaller;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;

public class OServerConfigurationLoaderXml {
  private final Class<? extends OServerConfiguration> rootClass;
  private JAXBContext context;
  private InputStream inputStream;
  private File file;
  private long fileLastModified = -1;

  public OServerConfigurationLoaderXml(
      final Class<? extends OServerConfiguration> iRootClass, final InputStream iInputStream) {
    rootClass = iRootClass;
    inputStream = iInputStream;
  }

  public OServerConfigurationLoaderXml(
      final Class<? extends OServerConfiguration> iRootClass, final File iFile) {
    rootClass = iRootClass;
    file = iFile;
  }

  public OServerConfiguration load() throws IOException {
    try {
      if (file != null) {
        fileLastModified = file.lastModified();

        String path = OFileUtils.getPath(file.getAbsolutePath());
        String current = OFileUtils.getPath(new File("").getAbsolutePath());
        if (path.startsWith(current)) path = path.substring(current.length() + 1);
        OLogManager.instance().info(this, "Loading configuration from: %s...", path);
      } else {
        OLogManager.instance().info(this, "Loading configuration from input stream");
      }

      context = JAXBContext.newInstance(rootClass);
      Unmarshaller unmarshaller = context.createUnmarshaller();
      unmarshaller.setSchema(null);

      final OServerConfiguration obj;

      if (file != null) {
        if (file.exists()) obj = rootClass.cast(unmarshaller.unmarshal(file));
        else {
          OLogManager.instance().error(this, "Server configuration file not found: %s", null, file);
          return rootClass.getConstructor(OServerConfigurationLoaderXml.class).newInstance(this);
        }
        obj.location = file.getAbsolutePath();
      } else {
        obj = rootClass.cast(unmarshaller.unmarshal(inputStream));
        obj.location = "memory";
      }

      // AUTO CONFIGURE SYSTEM CONFIGURATION
      OGlobalConfiguration config;
      if (obj.properties != null)
        for (OServerEntryConfiguration prop : obj.properties) {
          try {
            config = OGlobalConfiguration.findByKey(prop.name);
            if (config != null) {
              config.setValue(prop.value);
            }
          } catch (Exception e) {
          }
        }

      return obj;
    } catch (Exception e) {
      // SYNTAX ERROR? PRINT AN EXAMPLE
      OLogManager.instance()
          .error(this, "Invalid syntax. Below an example of how it should be:", e);

      try {
        context = JAXBContext.newInstance(rootClass);
        Marshaller marshaller = context.createMarshaller();
        marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
        Object example =
            rootClass.getConstructor(OServerConfigurationLoaderXml.class).newInstance(this);
        marshaller.marshal(example, System.out);
      } catch (Exception ex) {
      }

      throw new IOException(e);
    }
  }

  public void save(final OServerConfiguration iRootObject) throws IOException {
    if (file != null)
      try {
        context = JAXBContext.newInstance(rootClass);
        Marshaller marshaller = context.createMarshaller();
        marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
        marshaller.marshal(iRootObject, new FileWriter(file));
        fileLastModified = file.lastModified();
      } catch (JAXBException e) {
        throw new IOException(e);
      }
  }

  public File getFile() {
    return file;
  }

  public void setFile(File file) {
    this.file = file;
  }

  public boolean checkForAutoReloading() {
    if (file != null) return file.lastModified() > fileLastModified;

    return false;
  }
}
