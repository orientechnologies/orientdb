package com.orientechnologies.orient.core.storage.impl.local.paginated;

import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.storage.OStorage;

import java.io.IOException;

/**
 * @author Andrey Lomakin <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 7/15/14
 */
public class OStorageMemoryConfiguration extends OStorageConfiguration {
  private byte[] serializedContent;

  public OStorageMemoryConfiguration(OStorage iStorage) {
    super(iStorage);
  }

  public void close() throws IOException {
  }

  public void create() throws IOException {
  }

  @Override
  public OStorageConfiguration load() throws OSerializationException {
    try {
      fromStream(serializedContent);
    } catch (Exception e) {
      throw new OSerializationException("Cannot load database's configuration. The database seems to be corrupted.", e);
    }
    return this;
  }

  @Override
  public void lock() throws IOException {
  }

  @Override
  public void unlock() throws IOException {
  }

  @Override
  public void update() throws OSerializationException {
    try {
      serializedContent = toStream();
    } catch (Exception e) {
      throw new OSerializationException("Error on update storage configuration", e);
    }
  }

  public void synch() throws IOException {
  }

  @Override
  public void setSoftlyClosed(boolean softlyClosed) throws IOException {
  }

}