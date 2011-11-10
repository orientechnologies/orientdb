/*
 * Copyright 1999-2010 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.orient.server.config;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;

public class OServerConfigurationLoaderXml {
	private Class<? extends OServerConfiguration>	rootClass;
	private JAXBContext														context;
	private String																configurationText;
	private File																	file;

	public OServerConfigurationLoaderXml(Class<? extends OServerConfiguration> iRootClass, String iConfigurationText) {
		rootClass = iRootClass;
		configurationText = iConfigurationText;
	}

	public OServerConfigurationLoaderXml(Class<? extends OServerConfiguration> iRootClass, File iFile) {
		rootClass = iRootClass;
		file = iFile;
	}

	public OServerConfiguration load() throws IOException {
		try {
			context = JAXBContext.newInstance(rootClass);
			Unmarshaller unmarshaller = context.createUnmarshaller();
			unmarshaller.setSchema(null);

			final OServerConfiguration obj;

			if (file != null) {
				if (file.exists())
					obj = rootClass.cast(unmarshaller.unmarshal(file));
				else
					return rootClass.getConstructor(OServerConfigurationLoaderXml.class).newInstance(this);
				obj.location = file.getAbsolutePath();
			} else {
				obj = rootClass.cast(unmarshaller.unmarshal(new StringReader(configurationText)));
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
			OLogManager.instance().error(this, "Invalid syntax. Below an example of how it should be:", e);

			try {
				context = JAXBContext.newInstance(rootClass);
				Marshaller marshaller = context.createMarshaller();
				marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
				Object example = rootClass.getConstructor(OServerConfigurationLoaderXml.class).newInstance(this);
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
}
