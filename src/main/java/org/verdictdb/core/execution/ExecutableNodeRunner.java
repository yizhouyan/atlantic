package org.verdictdb.core.execution;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.verdictdb.core.connection.DbmsConnection;
import org.verdictdb.core.connection.DbmsQueryResult;
import org.verdictdb.core.sqlobject.SqlConvertable;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.sqlreader.QueryToSql;

public class ExecutableNodeRunner implements Runnable {

  DbmsConnection conn;

  ExecutableNode node;
  
  int successSourceCount = 0;
  
  int dependentCount;

  public ExecutableNodeRunner(DbmsConnection conn, ExecutableNode node) {
    this.conn = conn;
    this.node = node;
    this.dependentCount = node.getDependentNodeCount();
  }
  
  public static ExecutionInfoToken execute(DbmsConnection conn, ExecutableNode node) 
      throws VerdictDBException {
    return execute(conn, node, Arrays.<ExecutionInfoToken>asList());
  }
  
  public static ExecutionInfoToken execute(
      DbmsConnection conn, 
      ExecutableNode node, 
      List<ExecutionInfoToken> tokens) throws VerdictDBException {
    return (new ExecutableNodeRunner(conn, node)).execute(tokens);
  }

  @Override
  public void run() {
    // no dependency exists
    if (node.getSourceQueues().size() == 0) {
      try {
        executeAndBroadcast(Arrays.<ExecutionInfoToken>asList());
        broadcast(ExecutionInfoToken.successToken());
        return;
      } catch (VerdictDBException e) {
        e.printStackTrace();
        broadcast(ExecutionInfoToken.failureToken(e));
      }
    }
  
    // dependency exists
    while (true) {
      List<ExecutionInfoToken> tokens = retrieve();
      if (tokens == null) {
        continue;
      }
      if (doesIncludeFailure(tokens)) {
        broadcast(ExecutionInfoToken.failureToken());
        break;
      }
      if (areAllSuccess(tokens)) {
        broadcast(ExecutionInfoToken.successToken());
        break;
      }
      
      // actual processing
      try {
        executeAndBroadcast(tokens);
      } catch (VerdictDBException e) {
        e.printStackTrace();
        broadcast(ExecutionInfoToken.failureToken(e));
        break;
      }
    }
  }

  List<ExecutionInfoToken> retrieve() {
    List<ExecutionTokenQueue> sourceQueues = node.getSourceQueues();

    for (int i = 0; i < sourceQueues.size(); i++) {
      ExecutionInfoToken rs = sourceQueues.get(i).peek();
      if (rs == null) {
        return null;
      }
    }

    // all results available now
    List<ExecutionInfoToken> results = new ArrayList<>();
    for (int i = 0; i < sourceQueues.size(); i++) {
      ExecutionInfoToken rs = sourceQueues.get(i).take();
      results.add(rs);
    }
    return results;
  }

  void broadcast(ExecutionInfoToken token) {
//    System.out.println(new ToStringBuilder(node, ToStringStyle.DEFAULT_STYLE) + " broadcasts: " + token);
    for (ExecutionTokenQueue dest : node.getDestinationQueues()) {
      dest.add(token);
    }
  }
  
  void executeAndBroadcast(List<ExecutionInfoToken> tokens) throws VerdictDBException {
    ExecutionInfoToken resultToken = execute(tokens);
    if (resultToken != null) {
      broadcast(resultToken);
    }
  }

  public ExecutionInfoToken execute(List<ExecutionInfoToken> tokens) throws VerdictDBException {
    if (tokens.size() > 0 && tokens.get(0).isStatusToken()) {
      return null;
    }
    
    SqlConvertable sqlObj = node.createQuery(tokens);
    DbmsQueryResult intermediate = null;
    if (sqlObj != null) {
      String sql = QueryToSql.convert(conn.getSyntax(), sqlObj);
      intermediate = conn.execute(sql);
    }
    return node.createToken(intermediate);
  }

  boolean doesIncludeFailure(List<ExecutionInfoToken> tokens) {
    for (ExecutionInfoToken t : tokens) {
      if (t.isFailureToken()) {
        return true;
      }
    }
    return false;
  }

  boolean areAllSuccess(List<ExecutionInfoToken> latestResults) {
    for (ExecutionInfoToken t : latestResults) {
      if (t.isSuccessToken()) {
        successSourceCount++;
      } else {
        return false;
      }
    }
    
    if (successSourceCount == dependentCount) {
      return true;
    } else {
      return false;
    }
  }

}