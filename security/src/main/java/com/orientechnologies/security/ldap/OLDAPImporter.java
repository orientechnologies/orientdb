/**
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * <p>For more information: http://www.orientdb.com
 */
package com.orientechnologies.security.ldap;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.OrientDBInternal;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.security.OSecurityAuthenticator;
import com.orientechnologies.orient.core.security.OSecurityComponent;
import com.orientechnologies.orient.core.security.OSecuritySystem;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import javax.naming.directory.DirContext;
import javax.security.auth.Subject;

/**
 * Provides an LDAP importer.
 *
 * @author S. Colin Leister
 */
public class OLDAPImporter implements OSecurityComponent {
  private final String oldapUserClass = "_OLDAPUser";

  private boolean debug = false;
  private boolean enabled = true;

  private OrientDBInternal context;

  private int importPeriod = 60; // Default to 60
  // seconds.
  private Timer importTimer;

  // Used to track what roles are assigned to each database user.
  // Holds a map of the import databases and their corresponding dbGroups.
  private final ConcurrentHashMap<String, Database> databaseMap =
      new ConcurrentHashMap<String, Database>();

  private OSecuritySystem security;

  // OSecurityComponent
  public void active() {
    // Go through each database entry and check the _OLDAPUsers schema.
    for (Map.Entry<String, Database> dbEntry : databaseMap.entrySet()) {
      Database db = dbEntry.getValue();
      ODatabase<?> odb = null;

      try {
        odb = context.openNoAuthenticate(db.getName(), "internal");

        verifySchema(odb);
      } catch (Exception ex) {
        OLogManager.instance().error(this, "OLDAPImporter.active() Database: %s", ex, db.getName());
      } finally {
        if (odb != null) odb.close();
      }
    }

    ImportTask importTask = new ImportTask();
    importTimer = new Timer(true);
    importTimer.scheduleAtFixedRate(
        importTask, 30000, importPeriod * 1000); // Wait 30 seconds before starting

    OLogManager.instance().info(this, "**************************************");
    OLogManager.instance().info(this, "** OrientDB LDAP Importer Is Active **");
    OLogManager.instance().info(this, "**************************************");
  }

  // OSecurityComponent
  public void config(final ODocument importDoc, OSecuritySystem security) {
    try {
      context = security.getContext();
      this.security = security;

      databaseMap.clear();

      if (importDoc.containsField("debug")) {
        debug = importDoc.field("debug");
      }

      if (importDoc.containsField("enabled")) {
        enabled = importDoc.field("enabled");
      }

      if (importDoc.containsField("period")) {
        importPeriod = importDoc.field("period");

        if (debug) OLogManager.instance().info(this, "Import Period = " + importPeriod);
      }

      if (importDoc.containsField("databases")) {
        List<ODocument> list = importDoc.field("databases");

        for (ODocument dbDoc : list) {
          if (dbDoc.containsField("database")) {
            String dbName = dbDoc.field("database");

            if (debug) OLogManager.instance().info(this, "config() database: %s", dbName);

            boolean ignoreLocal = true;

            if (dbDoc.containsField("ignoreLocal")) {
              ignoreLocal = dbDoc.field("ignoreLocal");
            }

            if (dbDoc.containsField("domains")) {
              final List<DatabaseDomain> dbDomainsList = new ArrayList<DatabaseDomain>();

              final List<ODocument> dbdList = dbDoc.field("domains");

              for (ODocument dbDomainDoc : dbdList) {
                String domain = null;

                // "domain" is mandatory.
                if (dbDomainDoc.containsField("domain")) {
                  domain = dbDomainDoc.field("domain");

                  // If authenticator is null, it defaults to OLDAPImporter's primary
                  // OSecurityAuthenticator.
                  String authenticator = null;

                  if (dbDomainDoc.containsField("authenticator")) {
                    authenticator = dbDomainDoc.field("authenticator");
                  }

                  if (dbDomainDoc.containsField("servers")) {
                    final List<OLDAPServer> ldapServerList = new ArrayList<OLDAPServer>();

                    final List<ODocument> ldapServers = dbDomainDoc.field("servers");

                    for (ODocument ldapServerDoc : ldapServers) {
                      final String url = ldapServerDoc.field("url");

                      boolean isAlias = false;

                      if (ldapServerDoc.containsField("isAlias"))
                        isAlias = ldapServerDoc.field("isAlias");

                      OLDAPServer server = OLDAPServer.validateURL(url, isAlias);

                      if (server != null) {
                        ldapServerList.add(server);
                      } else {
                        OLogManager.instance()
                            .error(
                                this,
                                "Import LDAP Invalid server URL for database: %s, domain: %s, URL: %s",
                                null,
                                dbName,
                                domain,
                                url);
                      }
                    }

                    //
                    final List<User> userList = new ArrayList<User>();

                    final List<ODocument> userDocList = dbDomainDoc.field("users");

                    // userDocList can be null if only the oldapUserClass is used instead
                    // security.json.
                    if (userDocList != null) {
                      for (ODocument userDoc : userDocList) {
                        if (userDoc.containsField("baseDN") && userDoc.containsField("filter")) {
                          if (userDoc.containsField("roles")) {
                            final String baseDN = userDoc.field("baseDN");
                            final String filter = userDoc.field("filter");

                            if (debug)
                              OLogManager.instance()
                                  .info(
                                      this,
                                      "config() database: %s, baseDN: %s, filter: %s",
                                      dbName,
                                      baseDN,
                                      filter);

                            final List<String> roleList = userDoc.field("roles");

                            final User User = new User(baseDN, filter, roleList);

                            userList.add(User);
                          } else {
                            OLogManager.instance()
                                .error(
                                    this,
                                    "Import LDAP The User's \"roles\" property is missing for database %s",
                                    null);
                          }
                        } else {
                          OLogManager.instance()
                              .error(
                                  this,
                                  "Import LDAP The User's \"baseDN\" or \"filter\" property is missing for database %s",
                                  null);
                        }
                      }
                    }

                    DatabaseDomain dbd =
                        new DatabaseDomain(domain, ldapServerList, userList, authenticator);

                    dbDomainsList.add(dbd);
                  } else {
                    OLogManager.instance()
                        .error(
                            this,
                            "Import LDAP database %s \"domain\" is missing its \"servers\" property",
                            null);
                  }
                } else {
                  OLogManager.instance()
                      .error(
                          this,
                          "Import LDAP database %s \"domain\" object is missing its \"domain\" property",
                          null);
                }
              }

              if (dbName != null) {
                Database db = new Database(dbName, ignoreLocal, dbDomainsList);
                databaseMap.put(dbName, db);
              }
            } else {
              OLogManager.instance()
                  .error(this, "Import LDAP database %s contains no \"domains\" property", null);
            }
          } else {
            OLogManager.instance()
                .error(this, "Import LDAP databases contains no \"database\" property", null);
          }
        }
      } else {
        OLogManager.instance().error(this, "Import LDAP contains no \"databases\" property", null);
      }
    } catch (Exception ex) {
      OLogManager.instance().error(this, "OLDAPImporter.config()", ex);
    }
  }

  // OSecurityComponent
  public void dispose() {
    if (importTimer != null) {
      importTimer.cancel();
      importTimer = null;
    }
  }

  // OSecurityComponent
  public boolean isEnabled() {
    return enabled;
  }

  private void verifySchema(ODatabase<?> odb) {
    try {
      System.out.println("calling existsClass odb = " + odb);

      if (!odb.getMetadata().getSchema().existsClass(oldapUserClass)) {
        System.out.println("calling createClass");

        OClass ldapUser = odb.getMetadata().getSchema().createClass(oldapUserClass);

        System.out.println("calling createProperty");

        OProperty prop = ldapUser.createProperty("Domain", OType.STRING);

        System.out.println("calling setMandatory");

        prop.setMandatory(true);
        prop.setNotNull(true);

        prop = ldapUser.createProperty("BaseDN", OType.STRING);
        prop.setMandatory(true);
        prop.setNotNull(true);

        prop = ldapUser.createProperty("Filter", OType.STRING);
        prop.setMandatory(true);
        prop.setNotNull(true);

        prop = ldapUser.createProperty("Roles", OType.STRING);
        prop.setMandatory(true);
        prop.setNotNull(true);
      }
    } catch (Exception ex) {
      OLogManager.instance().error(this, "OLDAPImporter.verifySchema()", ex);
    }
  }

  private class Database {
    private String name;

    public String getName() {
      return name;
    }

    private boolean ignoreLocal;

    public boolean ignoreLocal() {
      return ignoreLocal;
    }

    private List<DatabaseDomain> databaseDomains;

    public List<DatabaseDomain> getDatabaseDomains() {
      return databaseDomains;
    }

    public Database(
        final String name, final boolean ignoreLocal, final List<DatabaseDomain> dbDomains) {
      this.name = name;
      this.ignoreLocal = ignoreLocal;
      databaseDomains = dbDomains;
    }
  }

  private class DatabaseDomain {
    private String domain;

    public String getDomain() {
      return domain;
    }

    private String authenticator;
    private List<OLDAPServer> ldapServers;
    private List<User> users;

    public String getAuthenticator() {
      return authenticator;
    }

    public List<OLDAPServer> getLDAPServers() {
      return ldapServers;
    }

    public List<User> getUsers() {
      return users;
    }

    public DatabaseDomain(
        final String domain,
        final List<OLDAPServer> ldapServers,
        final List<User> userList,
        String authenticator) {
      this.domain = domain;
      this.ldapServers = ldapServers;
      users = userList;
      this.authenticator = authenticator;
    }
  }

  private class DatabaseUser {
    private String user;
    private Set<String> roles = new LinkedHashSet<String>();

    private String getUser() {
      return user;
    }

    public Set<String> getRoles() {
      return roles;
    }

    public void addRoles(Set<String> roles) {
      if (roles != null) {
        for (String role : roles) {
          this.roles.add(role);
        }
      }
    }

    public DatabaseUser(final String user) {
      this.user = user;
    }
  }

  private class User {
    private String baseDN;
    private String filter;
    private Set<String> roles = new LinkedHashSet<String>();

    public String getBaseDN() {
      return baseDN;
    }

    public String getFilter() {
      return filter;
    }

    public Set<String> getRoles() {
      return roles;
    }

    public User(final String baseDN, final String filter, final List<String> roleList) {
      this.baseDN = baseDN;
      this.filter = filter;

      // Convert the list into a set, for convenience.
      for (String role : roleList) {
        roles.add(role);
      }
    }
  }

  // OSecuritySystemAccess
  public Subject getLDAPSubject(final String authName) {
    Subject subject = null;

    OSecurityAuthenticator authMethod = null;

    // If authName is null, use the primary authentication method.
    if (authName == null) authMethod = security.getPrimaryAuthenticator();
    else authMethod = security.getAuthenticator(authName);

    if (authMethod != null) subject = authMethod.getClientSubject();

    return subject;
  }

  /** * LDAP Import * */
  private synchronized void importLDAP() {
    if (security == null) {
      OLogManager.instance().error(this, "OLDAPImporter.importLDAP() ServerSecurity is null", null);
      return;
    }

    if (debug) OLogManager.instance().info(this, "OLDAPImporter.importLDAP() \n");

    for (Map.Entry<String, Database> dbEntry : databaseMap.entrySet()) {
      try {
        Database db = dbEntry.getValue();

        ODatabase<?> odb = context.openNoAuthenticate(db.getName(), "internal");

        // This set will be filled with all users from the database (unless ignoreLocal is true).
        // As each usersRetrieved list is filled, any matching user will be removed.
        // Once all the DatabaseGroups have been procesed, any remaining users in the set will be
        // deleted from the Database.
        Set<String> usersToBeDeleted = new LinkedHashSet<String>();

        Map<String, DatabaseUser> usersMap = new ConcurrentHashMap<String, DatabaseUser>();

        try {
          // We use this flag to determine whether to proceed with the call to deleteUsers() below.
          // If one or more LDAP servers cannot be reached (perhaps temporarily), we don't want to
          // delete all the database users, locking everyone out until the LDAP server is available
          // again.
          boolean deleteUsers = false;

          // Retrieves all the current OrientDB users from the specified ODatabase and stores them
          // in usersToBeDeleted.
          retrieveAllUsers(odb, db.ignoreLocal(), usersToBeDeleted);

          for (DatabaseDomain dd : db.getDatabaseDomains()) {
            try {
              Subject ldapSubject = getLDAPSubject(dd.getAuthenticator());

              if (ldapSubject != null) {
                DirContext dc = OLDAPLibrary.openContext(ldapSubject, dd.getLDAPServers(), debug);

                if (dc != null) {
                  deleteUsers = true;

                  try {
                    // Combine the "users" from security.json's "ldapImporter" and the class
                    // oldapUserClass.
                    List<User> userList = new ArrayList<User>();
                    userList.addAll(dd.getUsers());

                    retrieveLDAPUsers(odb, dd.getDomain(), userList);

                    for (User user : userList) {
                      List<String> usersRetrieved = new ArrayList<String>();

                      OLogManager.instance()
                          .info(
                              this,
                              "OLDAPImporter.importLDAP() Calling retrieveUsers for Database: %s, Filter: %s",
                              db.getName(),
                              user.getFilter());

                      OLDAPLibrary.retrieveUsers(
                          dc, user.getBaseDN(), user.getFilter(), usersRetrieved, debug);

                      if (!usersRetrieved.isEmpty()) {
                        for (String upn : usersRetrieved) {
                          if (usersToBeDeleted.contains(upn)) usersToBeDeleted.remove(upn);

                          OLogManager.instance()
                              .info(
                                  this,
                                  "OLDAPImporter.importLDAP() Database: %s, Filter: %s, UPN: %s",
                                  db.getName(),
                                  user.getFilter(),
                                  upn);

                          DatabaseUser dbUser = null;

                          if (usersMap.containsKey(upn)) dbUser = usersMap.get(upn);
                          else {
                            dbUser = new DatabaseUser(upn);
                            usersMap.put(upn, dbUser);
                          }

                          if (dbUser != null) {
                            dbUser.addRoles(user.getRoles());
                          }
                        }
                      } else {
                        OLogManager.instance()
                            .info(
                                this,
                                "OLDAPImporter.importLDAP() No users found at BaseDN: %s, Filter: %s, for Database: %s",
                                user.getBaseDN(),
                                user.getFilter(),
                                db.getName());
                      }
                    }
                  } finally {
                    dc.close();
                  }
                } else {
                  OLogManager.instance()
                      .error(
                          this,
                          "OLDAPImporter.importLDAP() Could not obtain an LDAP DirContext for Database %s",
                          null,
                          db.getName());
                }
              } else {
                OLogManager.instance()
                    .error(
                        this,
                        "OLDAPImporter.importLDAP() Could not obtain an LDAP Subject for Database %s",
                        null,
                        db.getName());
              }
            } catch (Exception ex) {
              OLogManager.instance()
                  .error(this, "OLDAPImporter.importLDAP() Database: %s", ex, db.getName());
            }
          }

          // Imports the LDAP users into the specified database, if it exists.
          importUsers(odb, usersMap);

          if (deleteUsers) deleteUsers(odb, usersToBeDeleted);
        } finally {
          if (usersMap != null) usersMap.clear();
          if (usersToBeDeleted != null) usersToBeDeleted.clear();
          if (odb != null) odb.close();
        }
      } catch (Exception ex) {
        OLogManager.instance().error(this, "OLDAPImporter.importLDAP()", ex);
      }
    }
  }

  // Loads the User object from the oldapUserClass class for each domain.
  // This is equivalent to the "users" objects in "ldapImporter" of security.json.
  private void retrieveLDAPUsers(
      final ODatabase<?> odb, final String domain, final List<User> userList) {
    try {
      String sql = String.format("SELECT FROM `%s` WHERE Domain = ?", oldapUserClass);

      OResultSet users = odb.query(sql, domain);

      while (users.hasNext()) {
        OResult userDoc = users.next();
        String roles = userDoc.getProperty("Roles");

        if (roles != null) {
          List<String> roleList = new ArrayList<String>();

          String[] roleArray = roles.split(",");

          for (String role : roleArray) {
            roleList.add(role.trim());
          }

          User user =
              new User(userDoc.getProperty("BaseDN"), userDoc.getProperty("Filter"), roleList);
          userList.add(user);
        } else {
          OLogManager.instance()
              .error(
                  this,
                  "OLDAPImporter.retrieveLDAPUsers() Roles is missing for entry Database: %s, Domain: %s",
                  null,
                  odb.getName(),
                  domain);
        }
      }
    } catch (Exception ex) {
      OLogManager.instance()
          .error(
              this,
              "OLDAPImporter.retrieveLDAPUsers() Database: %s, Domain: %s",
              ex,
              odb.getName(),
              domain);
    }
  }

  private void retrieveAllUsers(
      final ODatabase<?> odb, final boolean ignoreLocal, final Set<String> usersToBeDeleted) {
    try {
      String sql = "SELECT FROM OUser";

      if (ignoreLocal) sql = "SELECT FROM OUser WHERE _externalUser = true";
      OResultSet users = odb.query(sql);

      while (users.hasNext()) {
        OResult user = users.next();
        String name = user.getProperty("name");

        if (name != null) {
          if (!(name.equals("admin") || name.equals("reader") || name.equals("writer"))) {
            usersToBeDeleted.add(name);

            OLogManager.instance()
                .info(
                    this,
                    "OLDAPImporter.retrieveAllUsers() Database: %s, User: %s",
                    odb.getName(),
                    name);
          }
        }
      }
    } catch (Exception ex) {
      OLogManager.instance()
          .error(this, "OLDAPImporter.retrieveAllUsers() Database: %s", ex, odb.getName());
    }
  }

  private void deleteUsers(final ODatabase<?> odb, final Set<String> usersToBeDeleted) {
    try {
      for (String user : usersToBeDeleted) {
        odb.command("DELETE FROM OUser WHERE name = ?", user);

        OLogManager.instance()
            .info(
                this,
                "OLDAPImporter.deleteUsers() Deleted User: %s from Database: %s",
                user,
                odb.getName());
      }
    } catch (Exception ex) {
      OLogManager.instance()
          .error(this, "OLDAPImporter.deleteUsers() Database: %s", ex, odb.getName());
    }
  }

  private void importUsers(final ODatabase<?> odb, final Map<String, DatabaseUser> usersMap) {
    try {
      for (Map.Entry<String, DatabaseUser> entry : usersMap.entrySet()) {
        String upn = entry.getKey();

        if (upsertDbUser(odb, upn, entry.getValue().getRoles()))
          OLogManager.instance()
              .info(this, "Added/Modified Database User %s in Database %s", upn, odb.getName());
        else
          OLogManager.instance()
              .error(
                  this,
                  "Failed to add/update Database User %s in Database %s",
                  null,
                  upn,
                  odb.getName());
      }
    } catch (Exception ex) {
      OLogManager.instance()
          .error(this, "OLDAPImporter.importUsers() Database: %s", ex, odb.getName());
    }
  }

  /*
   * private boolean dbUserExists(ODatabase<?> db, String upn) { try { List<ODocument> list = new OSQLSynchQuery<ODocument>(
   * "SELECT FROM OUser WHERE name = ?").run(upn);
   *
   * return !list.isEmpty(); } catch(Exception ex) { OLogManager.instance().debug(this, "dbUserExists() Exception: ", ex); }
   *
   * return true; // Better to not add a user than to overwrite one. }
   */
  private boolean upsertDbUser(ODatabase<?> db, String upn, Set<String> roles) {
    try {
      // Create a random password to set for each imported user in case allowDefault is set to true.
      // We don't want blank or simple passwords set on the imported users, just in case.
      // final String password = OSecurityManager.instance().createSHA256(String.valueOf(new
      // java.util.Random().nextLong()));

      final String password = UUID.randomUUID().toString();

      StringBuilder sb = new StringBuilder();
      sb.append(
          "UPDATE OUser SET name = ?, password = ?, status = \"ACTIVE\", _externalUser = true, roles = (SELECT FROM ORole WHERE name in [");

      String[] roleParams = new String[roles.size()];

      Iterator<String> it = roles.iterator();

      int cnt = 0;

      while (it.hasNext()) {
        String role = it.next();

        sb.append("'");
        sb.append(role);
        sb.append("'");

        if (it.hasNext()) sb.append(", ");

        roleParams[cnt] = role;

        cnt++;
      }

      sb.append("]) UPSERT WHERE name = ?");

      db.command(sb.toString(), upn, password, upn);

      return true;
    } catch (Exception ex) {
      OLogManager.instance().error(this, "OLDAPImporter.upsertDbUser()", ex);
    }

    return false;
  }

  private class ImportTask extends TimerTask {
    @Override
    public void run() {
      importLDAP();
    }
  }
}
