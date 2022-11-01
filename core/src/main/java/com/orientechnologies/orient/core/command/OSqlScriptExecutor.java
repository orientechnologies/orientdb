package com.orientechnologies.orient.core.command;

import com.orientechnologies.orient.core.command.script.OCommandExecutorFunction;
import com.orientechnologies.orient.core.command.script.OCommandFunction;
import com.orientechnologies.orient.core.command.traverse.OAbstractScriptExecutor;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;
import com.orientechnologies.orient.core.sql.OSQLEngine;
import com.orientechnologies.orient.core.sql.executor.OInternalExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.sql.executor.ORetryExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.OScriptExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.RetryStep;
import com.orientechnologies.orient.core.sql.parser.OBeginStatement;
import com.orientechnologies.orient.core.sql.parser.OCommitStatement;
import com.orientechnologies.orient.core.sql.parser.OLetStatement;
import com.orientechnologies.orient.core.sql.parser.OLocalResultSet;
import com.orientechnologies.orient.core.sql.parser.OStatement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Created by tglman on 25/01/17. */
public class OSqlScriptExecutor extends OAbstractScriptExecutor {

  public OSqlScriptExecutor() {
    super("SQL");
  }

  @Override
  public OResultSet execute(ODatabaseDocumentInternal database, String script, Object... args)
      throws OCommandSQLParsingException, OCommandExecutionException {

    if (!script.trim().endsWith(";")) {
      script += ";";
    }
    List<OStatement> statements = OSQLEngine.parseScript(script, database);

    OCommandContext scriptContext = new OBasicCommandContext();
    ((OBasicCommandContext) scriptContext).setDatabase(database);
    Map<Object, Object> params = new HashMap<>();
    if (args != null) {
      for (int i = 0; i < args.length; i++) {
        params.put(i, args[i]);
      }
    }
    scriptContext.setInputParameters(params);

    return executeInternal(statements, scriptContext);
  }

  @Override
  public OResultSet execute(ODatabaseDocumentInternal database, String script, Map params) {
    if (!script.trim().endsWith(";")) {
      script += ";";
    }
    List<OStatement> statements = OSQLEngine.parseScript(script, database);

    OCommandContext scriptContext = new OBasicCommandContext();
    ((OBasicCommandContext) scriptContext).setDatabase(database);

    scriptContext.setInputParameters(params);

    return executeInternal(statements, scriptContext);
  }

  private OResultSet executeInternal(List<OStatement> statements, OCommandContext scriptContext) {
    OScriptExecutionPlan plan = new OScriptExecutionPlan(scriptContext);

    plan.setStatement(
        statements.stream().map(OStatement::toString).collect(Collectors.joining(";")));

    List<OStatement> lastRetryBlock = new ArrayList<>();
    int nestedTxLevel = 0;

    for (OStatement stm : statements) {
      if (stm.getOriginalStatement() == null) {
        stm.setOriginalStatement(stm.toString());
      }
      if (stm instanceof OBeginStatement) {
        nestedTxLevel++;
      }

      if (nestedTxLevel <= 0) {
        OInternalExecutionPlan sub = stm.createExecutionPlan(scriptContext);
        plan.chain(sub, false);
      } else {
        lastRetryBlock.add(stm);
      }

      if (stm instanceof OCommitStatement && nestedTxLevel > 0) {
        nestedTxLevel--;
        if (nestedTxLevel == 0) {
          if (((OCommitStatement) stm).getRetry() != null) {
            int nRetries = ((OCommitStatement) stm).getRetry().getValue().intValue();
            if (nRetries <= 0) {
              throw new OCommandExecutionException("Invalid retry number: " + nRetries);
            }

            RetryStep step =
                new RetryStep(
                    lastRetryBlock,
                    nRetries,
                    ((OCommitStatement) stm).getElseStatements(),
                    ((OCommitStatement) stm).getElseFail(),
                    scriptContext,
                    false);
            ORetryExecutionPlan retryPlan = new ORetryExecutionPlan(scriptContext);
            retryPlan.chain(step);
            plan.chain(retryPlan, false);
            lastRetryBlock = new ArrayList<>();
          } else {
            for (OStatement statement : lastRetryBlock) {
              OInternalExecutionPlan sub = statement.createExecutionPlan(scriptContext);
              plan.chain(sub, false);
            }
          }
        }
      }

      if (stm instanceof OLetStatement) {
        scriptContext.declareScriptVariable(((OLetStatement) stm).getName().getStringValue());
      }
    }
    return new OLocalResultSet(plan);
  }

  @Override
  public Object executeFunction(
      OCommandContext context, final String functionName, final Map<Object, Object> iArgs) {

    ODatabaseDocumentInternal db = (ODatabaseDocumentInternal) context.getDatabase();
    if (db == null) {
      db = ODatabaseRecordThreadLocal.instance().get();
    }

    final OCommandExecutorFunction command = new OCommandExecutorFunction();
    command.parse(new OCommandFunction(functionName));
    return command.executeInContext(context, iArgs);
  }
}
