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

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.validation.Schema;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;

public class OServerConfigurationLoaderXml {
	private Class<? extends OServerConfiguration>	rootClass;
	private String																filePath;
	private JAXBContext														context;
	private File																	file;

	public OServerConfigurationLoaderXml(Class<? extends OServerConfiguration> iRootClass, String iFilePath) {
		filePath = OSystemVariableResolver.resolveSystemVariables(iFilePath);
		rootClass = iRootClass;
		file = new File(filePath);
		filePath = iFilePath;
	}

	public OServerConfiguration load() throws IOException {
		try {
			if (file.exists()) {
				context = JAXBContext.newInstance(rootClass);
				Unmarshaller unmarshaller = context.createUnmarshaller();

				Schema schema = null; // SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI).newSchema(new File("Item.xsd"));

				unmarshaller.setSchema(schema);
				OServerConfiguration obj = rootClass.cast(unmarshaller.unmarshal(file));
				obj.location = filePath;

				// AUTO CONFIGURE SYSTEM CONFIGURATION
				OGlobalConfiguration config;
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
			} else
				return rootClass.getConstructor(OServerConfigurationLoaderXml.class, String.class).newInstance(this, filePath);
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
				throw new IOException(ex);
			}

			throw new IOException(e);
		}
	}

	public void save(final OServerConfiguration iRootObject) throws IOException {
		try {
			context = JAXBContext.newInstance(rootClass);
			Marshaller marshaller = context.createMarshaller();
			marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
			marshaller.marshal(iRootObject, new FileWriter(filePath));
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
