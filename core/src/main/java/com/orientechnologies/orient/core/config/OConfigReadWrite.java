package com.orientechnologies.orient.core.config;

import com.fasterxml.jackson.core.exc.StreamWriteException;
import com.fasterxml.jackson.databind.DatabindException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import java.io.IOException;
import java.nio.file.Path;

public class OConfigReadWrite {

  public OConfig readJSON(Path file) throws IOException {
    return read(file, new ObjectMapper());
  }

  public void writeJSON(Path file, OConfig config) throws IOException {
    write(file, config, new ObjectMapper());
  }

  public OConfig readYML(Path file) throws IOException {
    return read(file, new YAMLMapper());
  }

  public void writeYML(Path file, OConfig config) throws IOException {
    write(file, config, new YAMLMapper());
  }

  protected OConfig read(Path file, ObjectMapper mapper) throws IOException {
    var reader = mapper.readerFor(OConfig.class);
    return reader.readValue(file.toFile());
  }

  protected void write(Path file, OConfig config, ObjectMapper mapper)
      throws IOException, StreamWriteException, DatabindException {
    var writer = mapper.writerFor(OConfig.class);
    writer.writeValue(file.toFile(), config);
  }

  public OConfig readYMLString(String data) throws IOException {
    return readString(data, new YAMLMapper());
  }

  public String writeYMLString(OConfig config) throws IOException {
    return writeString(config, new YAMLMapper());
  }

  protected OConfig readString(String data, ObjectMapper mapper) throws IOException {
    var reader = mapper.readerFor(OConfig.class);
    return reader.readValue(data);
  }

  protected String writeString(OConfig config, ObjectMapper mapper)
      throws IOException, StreamWriteException, DatabindException {
    var writer = mapper.writerFor(OConfig.class);
    return writer.writeValueAsString(config);
  }
}
