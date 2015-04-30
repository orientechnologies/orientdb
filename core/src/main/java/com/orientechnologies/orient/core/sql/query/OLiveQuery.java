package com.orientechnologies.orient.core.sql.query;

/**
 * Created by luigidellaquila on 23/03/15.
 */
public class OLiveQuery<T> extends OSQLSynchQuery<T> {

  public OLiveQuery() {
  }

  public OLiveQuery(String iText, final OLiveResultListener iResultListener) {
    super(iText);
    setResultListener(new OLocalLiveResultListener(iResultListener));
  }

  @Override
  public <RET> RET execute(Object... iArgs) {
    return super.execute(iArgs);
  }
}
