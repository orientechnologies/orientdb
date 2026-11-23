package com.orientechnologies.orient.core.osystem;

import com.orientechnologies.orient.core.db.OSystemDatabase;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.security.OImmutableUser;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.OSecurity;
import com.orientechnologies.orient.core.metadata.security.OSecurityUser;
import com.orientechnologies.orient.core.metadata.security.OSystemUser;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.ArrayList;
import java.util.List;

public class OSystemUsersImpl implements OSystemUsers {

  protected static final String USER_FIELD_NAME = "name";
  private static final String USER_FIELD_PASSWORD = "password";
  private static final String USER_FIELD_STATUS = "status";
  private static final String USER_FIELD_ROLES = "roles";

  private static final String DEFAULT_STATUS = "ACTIVE";
  private static final String DEFAULT_ROLE = "writer";
  private static final String ROLE_CLASS = "ORole";
  private static final String ROLE_FIELD_NAME = "name";

  private final OSystemDatabase system;

  public OSystemUsersImpl(OSystemDatabase system) {
    this.system = system;
  }

  public OSecurityUser getSystemUser(final String username, final String dbName) {
    if (system.exists()) {
      return (OImmutableUser)
          system.execute(
              (resultset) -> {
                if (resultset != null && resultset.hasNext())
                  return new OImmutableUser(
                      0,
                      new OSystemUser(
                          (ODocument) resultset.next().getElement().get().getRecord(), dbName));
                return null;
              },
              "select from OUser where name = ? limit 1 fetchplan roles:1",
              username);
    }
    return null;
  }

  public void updateSystemUserPassword(String user, String pwd) {
    system.executeWithDB(
        (db) -> {
          db.command("update OUser set password= ? WHERE name = ?", pwd, user).close();
          return (Void) null;
        });
  }

  public boolean dropSystemUser(String user) {
    return system.executeWithDB(
        db -> {
          List<Object> params = new ArrayList<>();
          // INSERT INTO OUser SET
          StringBuilder sb = new StringBuilder();
          sb.append("DELETE FROM OUser WHERE ");

          sb.append(USER_FIELD_NAME);
          sb.append("=?");
          params.add(user);

          var stream = db.command(sb.toString(), params.toArray());
          if (stream.hasNext()) {
            var res = stream.next();
            if (Integer.valueOf(1).equals(res.getProperty("count"))) {
              return true;
            }
          }
          return false;
        });
  }

  public boolean existsSystemUser(String user) {
    return system.executeWithDB(
        db -> {
          List<Object> params = new ArrayList<>();
          params.add(user);
          // INSERT INTO OUser SET
          StringBuilder sb = new StringBuilder();
          sb.append("SELECT FROM OUser WHERE name = ?");

          try (OResultSet rs = db.command(sb.toString(), params.toArray())) {
            if (rs.hasNext()) {
              return true;
            } else {
              return false;
            }
          }
        });
  }

  @Override
  public OUser createSystemUser(String user, String password, List<String> roles) {
    return system.executeWithDB(
        (db) -> {
          List<Object> params = new ArrayList<>();
          // INSERT INTO OUser SET
          StringBuilder sb = new StringBuilder();
          sb.append("INSERT INTO OUser SET ");

          sb.append(USER_FIELD_NAME);
          sb.append("=?");
          params.add(user);

          // pass=<pass>
          sb.append(',');
          sb.append(USER_FIELD_PASSWORD);
          sb.append("=");
          sb.append("?");
          params.add(password);

          // status=ACTIVE
          sb.append(',');
          sb.append(USER_FIELD_STATUS);
          sb.append("='");
          sb.append(DEFAULT_STATUS);
          sb.append("'");

          // role=(select from ORole where name in [<input_role || 'writer'>)]
          if (roles.size() == 0) {
            roles.add(DEFAULT_ROLE);
          }

          sb.append(',');
          sb.append(USER_FIELD_ROLES);
          sb.append("=(SELECT FROM ");
          sb.append(ROLE_CLASS);
          sb.append(" WHERE ");
          sb.append(ROLE_FIELD_NAME);
          sb.append(" IN [");
          OSecurity security = db.getMetadata().getSecurity();
          for (int i = 0; i < roles.size(); ++i) {
            String roleName = roles.get(i);
            ORole role = security.getRole(roleName);
            if (role == null) {
              throw new OCommandExecutionException(
                  "Cannot create user " + user + ": role " + roleName + " does not exist");
            }
            if (i > 0) {
              sb.append(", ");
            }

            if (roleName.startsWith("'") || roleName.startsWith("\"")) {
              sb.append(roleName);
            } else {
              sb.append("'");
              sb.append(roleName);
              sb.append("'");
            }
          }
          sb.append("])");
          var res = db.command(sb.toString(), params.toArray());
          if (res.hasNext()) {
            return new OSystemUser((ODocument) res.next().toElement());
          } else {
            return null;
          }
        });
  }
}
