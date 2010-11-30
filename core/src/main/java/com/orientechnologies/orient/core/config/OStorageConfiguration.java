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
package com.orientechnologies.orient.core.config;

import java.io.IOException;
import java.text.DateFormat;
import java.text.DecimalFormatSymbols;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import com.orientechnologies.orient.core.serialization.OSerializableStream;
import com.orientechnologies.orient.core.storage.OStorage;

public class OStorageConfiguration implements OSerializableStream {
	public static final int										CONFIG_RECORD_NUM	= 0;

	public int																version						= 0;
	public String															name;
	public String															schemaRecordId;
	public String															dictionaryRecordId;

	public String															localeLanguage		= Locale.getDefault().getLanguage();
	public String															localeCountry			= Locale.getDefault().getCountry();
	public String															dateFormat				= "yyyy-MM-dd";
	public String															dateTimeFormat		= "yyyy-MM-dd hh:mm:ss";

	public List<OStorageClusterConfiguration>	clusters					= new ArrayList<OStorageClusterConfiguration>();
	public List<OStorageDataConfiguration>		dataSegments			= new ArrayList<OStorageDataConfiguration>();

	public OStorageTxConfiguration						txSegment					= new OStorageTxConfiguration();

	public List<OEntryConfiguration>					properties				= new ArrayList<OEntryConfiguration>();

	private transient Locale									localeInstance;
	private transient DateFormat							dateFormatInstance;
	private transient DateFormat							dateTimeFormatInstance;
	private transient DecimalFormatSymbols		unusualSymbols;
	private transient OStorage								storage;
	private transient byte[]									record;
	private int																fixedSize					= 0;

	public OStorageConfiguration load() throws OSerializationException {
		record = storage.readRecord(null, -1, storage.getClusterIdByName(OStorage.CLUSTER_INTERNAL_NAME), CONFIG_RECORD_NUM, null).buffer;
		fixedSize = record.length;

		if (record == null)
			throw new OStorageException("Can't load database's configuration. The database seems to be corrupted.");

		fromStream(record);
		return this;
	}

	public void update() throws OSerializationException {
		if (record == null)
			return;

		record = toStream();
		storage.updateRecord(-1, storage.getClusterIdByName(OStorage.CLUSTER_INTERNAL_NAME), 0, record, -1, ORecordBytes.RECORD_TYPE);
	}

	public void create(final int iFixedSize) throws IOException {
		fixedSize = iFixedSize;
		record = toStream();
		storage.createRecord(storage.getClusterIdByName(OStorage.CLUSTER_INTERNAL_NAME), record, ORecordBytes.RECORD_TYPE);
	}

	public OStorageConfiguration(final OStorage iStorage) {
		storage = iStorage;
	}

	public boolean isEmpty() {
		return clusters.isEmpty();
	}

	public Locale getLocaleInstance() {
		if (localeInstance == null)
			localeInstance = new Locale(localeLanguage, localeCountry);

		return localeInstance;
	}

	public DateFormat getDateFormatInstance() {
		if (dateFormatInstance == null) {
			dateFormatInstance = new SimpleDateFormat(dateFormat);
			dateFormatInstance.setLenient(false);
		}
		return dateFormatInstance;
	}

	public DateFormat getDateTimeFormatInstance() {
		if (dateTimeFormatInstance == null) {
			dateTimeFormatInstance = new SimpleDateFormat(dateTimeFormat);
			dateTimeFormatInstance.setLenient(false);
		}
		return dateTimeFormatInstance;
	}

	public DecimalFormatSymbols getUnusualSymbols() {
		if (unusualSymbols == null)
			unusualSymbols = new DecimalFormatSymbols(getLocaleInstance());
		return unusualSymbols;
	}

	public OSerializableStream fromStream(byte[] iStream) throws OSerializationException {
		String[] values = new String(iStream).split("\\|");
		int index = 0;
		version = Integer.parseInt(read(values[index++]));
		name = read(values[index++]);

		schemaRecordId = read(values[index++]);
		dictionaryRecordId = read(values[index++]);

		localeLanguage = read(values[index++]);
		localeCountry = read(values[index++]);
		dateFormat = read(values[index++]);
		dateTimeFormat = read(values[index++]);

		int size = Integer.parseInt(read(values[index++]));
		String clusterType;
		int clusterId;
		String clusterName;

		// PREPARE THE LIST OF CLUSTERS
		clusters = new ArrayList<OStorageClusterConfiguration>(size);
		for (int i = 0; i < size; ++i)
			clusters.add(null);

		OStoragePhysicalClusterConfiguration phyCluster;
		OStorageLogicalClusterConfiguration logCluster;
		OStorageMemoryClusterConfiguration memCluster;

		for (int i = 0; i < size; ++i) {
			clusterId = Integer.parseInt(read(values[index++]));
			clusterName = read(values[index++]);

			clusterType = read(values[index++]);

			// PHYSICAL CLUSTER
			if (clusterType.equals("p")) {
				phyCluster = new OStoragePhysicalClusterConfiguration(this, clusterId);
				phyCluster.name = clusterName;
				index = phySegmentFromStream(values, index, phyCluster);
				phyCluster.holeFile = new OStorageClusterHoleConfiguration(phyCluster, read(values[index++]), read(values[index++]),
						read(values[index++]));
				clusters.set(clusterId, phyCluster);
			} else if (clusterType.equals("l")) {
				// LOGICAL CLUSTER
				logCluster = new OStorageLogicalClusterConfiguration(clusterName, clusterId, Integer.parseInt(read(values[index++])),
						new ORecordId(values[index++]));
				clusters.set(clusterId, logCluster);
			} else {
				// MEMORY CLUSTER
				memCluster = new OStorageMemoryClusterConfiguration(clusterName, clusterId);
				clusters.set(clusterId, memCluster);
			}
		}

		// PREPARE THE LIST OF DATA SEGS
		size = Integer.parseInt(read(values[index++]));
		dataSegments = new ArrayList<OStorageDataConfiguration>(size);
		for (int i = 0; i < size; ++i)
			dataSegments.add(null);

		int dataId;
		String dataName;
		OStorageDataConfiguration data;
		for (int i = 0; i < size; ++i) {
			dataId = Integer.parseInt(read(values[index++]));
			dataName = read(values[index++]);

			data = new OStorageDataConfiguration(this, dataName);
			index = phySegmentFromStream(values, index, data);
			data.holeFile = new OStorageDataHoleConfiguration(data, read(values[index++]), read(values[index++]), read(values[index++]));
			dataSegments.set(dataId, data);
		}

		txSegment = new OStorageTxConfiguration(read(values[index++]), read(values[index++]), read(values[index++]),
				read(values[index++]), read(values[index++]));

		size = Integer.parseInt(read(values[index++]));
		properties = new ArrayList<OEntryConfiguration>(size);
		for (int i = 0; i < size; ++i) {
			properties.add(new OEntryConfiguration(read(values[index++]), read(values[index++])));
		}

		return this;
	}

	public byte[] toStream() throws OSerializationException {
		StringBuilder buffer = new StringBuilder();

		write(buffer, version);
		write(buffer, name);

		write(buffer, schemaRecordId);
		write(buffer, dictionaryRecordId);

		write(buffer, localeLanguage);
		write(buffer, localeCountry);
		write(buffer, dateFormat);
		write(buffer, dateTimeFormat);

		write(buffer, clusters.size());
		for (OStorageClusterConfiguration c : clusters) {
			if (c == null)
				continue;

			write(buffer, c.getId());
			write(buffer, c.getName());

			if (c instanceof OStoragePhysicalClusterConfiguration) {
				// PHYSICAL
				write(buffer, "p");
				phySegmentToStream(buffer, (OStoragePhysicalClusterConfiguration) c);
				fileToStream(buffer, ((OStoragePhysicalClusterConfiguration) c).holeFile);
			} else if (c instanceof OStorageLogicalClusterConfiguration) {
				// LOGICAL
				write(buffer, "l");
				logSegmentToStream(buffer, (OStorageLogicalClusterConfiguration) c);
			} else {
				// MEMORY
				write(buffer, "m");
			}
		}

		write(buffer, dataSegments.size());
		for (OStorageDataConfiguration d : dataSegments) {
			if (d == null)
				continue;

			write(buffer, d.id);
			write(buffer, d.name);

			phySegmentToStream(buffer, d);
			fileToStream(buffer, d.holeFile);
		}

		fileToStream(buffer, txSegment);
		write(buffer, txSegment.isSynchRecord());
		write(buffer, txSegment.isSynchTx());

		write(buffer, properties.size());
		for (OEntryConfiguration e : properties)
			entryToStream(buffer, e);

		if (fixedSize > 0 && buffer.length() > fixedSize)
			throw new OConfigurationException("Configuration data exceeded size limit: " + fixedSize + " bytes");

		// ALLOCATE ENOUGHT SPACE TO REUSE IT EVERY TIME
		buffer.append("|");
		if (fixedSize > 0)
			buffer.setLength(fixedSize);

		return buffer.toString().getBytes();
	}

	private int phySegmentFromStream(final String[] values, int index, final OStorageSegmentConfiguration iSegment) {
		iSegment.maxSize = read(values[index++]);
		iSegment.fileType = read(values[index++]);
		iSegment.fileStartSize = read(values[index++]);
		iSegment.fileMaxSize = read(values[index++]);
		iSegment.fileIncrementSize = read(values[index++]);
		iSegment.defrag = read(values[index++]);

		final int size = Integer.parseInt(read(values[index++]));
		iSegment.infoFiles = new OStorageFileConfiguration[size];
		for (int i = 0; i < size; ++i) {
			iSegment.infoFiles[i] = new OStorageFileConfiguration(iSegment, read(values[index++]), read(values[index++]),
					read(values[index++]), iSegment.fileIncrementSize);
		}

		return index;
	}

	private void phySegmentToStream(final StringBuilder iBuffer, final OStorageSegmentConfiguration iSegment) {
		write(iBuffer, iSegment.maxSize);
		write(iBuffer, iSegment.fileType);
		write(iBuffer, iSegment.fileStartSize);
		write(iBuffer, iSegment.fileMaxSize);
		write(iBuffer, iSegment.fileIncrementSize);
		write(iBuffer, iSegment.defrag);

		write(iBuffer, iSegment.infoFiles.length);
		for (OStorageFileConfiguration f : iSegment.infoFiles)
			fileToStream(iBuffer, f);
	}

	private void logSegmentToStream(final StringBuilder iBuffer, final OStorageLogicalClusterConfiguration iSegment) {
		write(iBuffer, iSegment.physicalClusterId);
		write(iBuffer, iSegment.map.toString());
	}

	private void fileToStream(final StringBuilder iBuffer, final OStorageFileConfiguration iFile) {
		write(iBuffer, iFile.path);
		write(iBuffer, iFile.type);
		write(iBuffer, iFile.maxSize);
	}

	private void entryToStream(final StringBuilder iBuffer, final OEntryConfiguration iEntry) {
		write(iBuffer, iEntry.name);
		write(iBuffer, iEntry.value);
	}

	private String read(final String iValue) {
		if (iValue.equals(" "))
			return null;
		return iValue;
	}

	private void write(final StringBuilder iBuffer, final Object iValue) {
		if (iBuffer.length() > 0)
			iBuffer.append("|");
		iBuffer.append(iValue != null ? iValue.toString() : " ");
	}
}
