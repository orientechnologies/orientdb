package com.orientechnologies.orient.server.handler;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.handler.OAutomaticBackup.MODE;
import java.nio.file.Path;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

public class OAtomaticBackupConfig {
  private boolean enabled;
  private long delay = -1;
  private Date firstTime = null;
  private Path targetDirectory = Path.of("backup");
  private Set<String> dbInclude = Collections.emptySet();
  private Set<String> dbExclude = Collections.emptySet();
  private String targetFileName;
  private int bufferSize = 1048576;
  private int compressionLevel = 9;
  private MODE mode = MODE.FULL_BACKUP;
  private String exportOptions;

  public static OAtomaticBackupConfig newFromParams(OServerParameterConfiguration[] iParams) {
    OAtomaticBackupConfig conf = new OAtomaticBackupConfig();
    // LEGACY <v2.2: CONVERT ALL SETTINGS IN JSON
    for (OServerParameterConfiguration param : iParams) {
      if (param.getName().equalsIgnoreCase("enabled")) {
        conf.enabled = Boolean.parseBoolean(param.getValue());
      } else if (param.getName().equalsIgnoreCase("delay")) {
        conf.delay = OIOUtils.getTimeAsMillisecs(param.getValue());
      } else if (param.getName().equalsIgnoreCase("firstTime")) {
        try {
          conf.firstTime = OIOUtils.getTodayWithTime(param.getValue());
          if (conf.firstTime.before(new Date())) {
            Calendar cal = Calendar.getInstance();
            cal.setTime(conf.firstTime);
            cal.add(Calendar.DAY_OF_MONTH, 1);
            conf.firstTime = cal.getTime();
          }
        } catch (ParseException e) {
          throw OException.wrapException(
              new OConfigurationException(
                  "Parameter 'firstTime' has invalid format, expected: HH:mm:ss"),
              e);
        }
      } else if (param.getName().equalsIgnoreCase("target.directory")) {
        conf.targetDirectory = Path.of(param.getValue()).normalize();
      } else if (param.getName().equalsIgnoreCase("db.include")
          && param.getValue().trim().length() > 0) {
        conf.dbInclude = new HashSet<>(Arrays.asList(param.getValue().split(",")));
      } else if (param.getName().equalsIgnoreCase("db.exclude")
          && param.getValue().trim().length() > 0) {
        conf.dbExclude = new HashSet<>(Arrays.asList(param.getValue().split(",")));
      } else if (param.getName().equalsIgnoreCase("target.fileName")) {
        conf.targetFileName = param.getValue();
      } else if (param.getName().equalsIgnoreCase("bufferSize")) {
        conf.bufferSize = Integer.parseInt(param.getValue());
      } else if (param.getName().equalsIgnoreCase("compressionLevel")) {
        conf.compressionLevel = Integer.parseInt(param.getValue());
      } else if (param.getName().equalsIgnoreCase("mode")) {
        conf.mode = MODE.valueOf(param.getValue().toUpperCase(Locale.ENGLISH));
      } else if (param.getName().equalsIgnoreCase("exportOptions")) {
        conf.exportOptions = param.getValue();
      }
    }
    return conf;
  }

  public static OAtomaticBackupConfig newFromODocument(ODocument config) {
    OAtomaticBackupConfig conf = new OAtomaticBackupConfig();
    // LEGACY <v2.2: CONVERT ALL SETTINGS IN JSON
    for (String param : config.fieldNames()) {
      final Object val = config.getProperty(param);
      if (val == null) {
        continue;
      }
      String stringVal = val.toString();
      if (param.equalsIgnoreCase("enabled")) {
        conf.enabled = Boolean.parseBoolean(stringVal);
      } else if (param.equalsIgnoreCase("delay")) {
        conf.delay = OIOUtils.getTimeAsMillisecs(stringVal);
      } else if (param.equalsIgnoreCase("firstTime")) {
        try {
          conf.firstTime = OIOUtils.getTodayWithTime(stringVal);
          if (conf.firstTime.before(new Date())) {
            Calendar cal = Calendar.getInstance();
            cal.setTime(conf.firstTime);
            cal.add(Calendar.DAY_OF_MONTH, 1);
            conf.firstTime = cal.getTime();
          }
        } catch (ParseException e) {
          throw OException.wrapException(
              new OConfigurationException(
                  "Parameter 'firstTime' has invalid format, expected: HH:mm:ss"),
              e);
        }
      } else if (param.equalsIgnoreCase("targetDirectory")) {
        conf.targetDirectory = Path.of(stringVal).normalize();
      } else if (param.equalsIgnoreCase("dbInclude") && param.trim().length() > 0) {
        if (val instanceof Set) {
          conf.dbInclude = (Set<String>) val;
        } else if (val instanceof List) {
          conf.dbInclude = new HashSet<>((List<String>) val);
        } else if (stringVal != null) {
          conf.dbInclude = new HashSet<>(Arrays.asList(stringVal.split(",")));
        }
      } else if (param.equalsIgnoreCase("dbExclude") && param.trim().length() > 0) {
        if (val instanceof Set) {
          conf.dbExclude = (Set<String>) val;
        } else if (val instanceof List) {
          conf.dbExclude = new HashSet<>((List<String>) val);
        } else if (stringVal != null) {
          conf.dbExclude = new HashSet<>(Arrays.asList(stringVal.split(",")));
        }
      } else if (param.equalsIgnoreCase("targetFileName")) {
        conf.targetFileName = stringVal;
      } else if (param.equalsIgnoreCase("bufferSize")) {
        conf.bufferSize = Integer.parseInt(stringVal);
      } else if (param.equalsIgnoreCase("compressionLevel")) {
        conf.compressionLevel = Integer.parseInt(stringVal);
      } else if (param.equalsIgnoreCase("mode")) {
        conf.mode = MODE.valueOf(stringVal.toUpperCase(Locale.ENGLISH));
      } else if (param.equalsIgnoreCase("exportOptions")) {
        conf.exportOptions = stringVal;
      }
    }
    return conf;
  }

  public boolean isEnabled() {
    return enabled;
  }

  public long getDelay() {
    return delay;
  }

  public Date getFirstTime() {
    return firstTime;
  }

  public Path getTargetDirectory() {
    return targetDirectory;
  }

  public Set<String> getDbInclude() {
    return dbInclude;
  }

  public Set<String> getDbExclude() {
    return dbExclude;
  }

  public String getTargetFileName() {
    return targetFileName;
  }

  public int getBufferSize() {
    return bufferSize;
  }

  public int getCompressionLevel() {
    return compressionLevel;
  }

  public MODE getMode() {
    return mode;
  }

  public String getExportOptions() {
    return exportOptions;
  }

  public ODocument toDocument() {
    ODocument doc = new ODocument();
    doc.setProperty("enabled", enabled);
    doc.setProperty("delay", delay);
    doc.setProperty("firstTime", firstTime);
    doc.setProperty("target.directory", targetDirectory.toString());
    doc.setProperty("db.include", dbInclude);
    doc.setProperty("db.exclude", dbExclude);
    doc.setProperty("target.fileName", targetFileName);
    doc.setProperty("bufferSize", bufferSize);
    doc.setProperty("compressionLevel", compressionLevel);
    doc.setProperty("mode", mode.toString());
    doc.setProperty("exportOptions", exportOptions);
    return doc;
  }
}
