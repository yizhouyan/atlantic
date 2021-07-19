/*
 *    Copyright 2018 University of Michigan
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package org.verdictdb.jdbc41;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;

import org.verdictdb.commons.VerdictDBLogger;
import org.verdictdb.coordinator.ExecutionContext;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.VerdictContext;
import org.verdictdb.VerdictResultStream;
import org.verdictdb.VerdictSingleResult;

public class VerdictStatement implements java.sql.Statement {

  Connection conn;

  ExecutionContext executionContext;

  VerdictSingleResult result;

  private VerdictDBLogger log = VerdictDBLogger.getLogger(this.getClass());

  public VerdictStatement(Connection conn, VerdictContext context) {
    this.conn = conn;
    this.executionContext = context.createNewExecutionContext();
  }

  /**
   * Created by Shucheng Zhong on 9/13/18
   * <p>
   * It will try to get the VerdictSingleResult from VerdictResultStream
   * and append the VerdictSingleResult to VerdictStreamResultSet
   * until all the VerdictSingleResults have return.
   * Since it running on separate thread, it won't block user querying from
   * the ResultSet
   */
  class ExecuteStream implements Runnable {

    VerdictResultStream resultStream;

    VerdictStreamResultSet resultSet;

    ExecutionContext executionContext;

    ExecuteStream(VerdictResultStream resultStream, VerdictStreamResultSet resultSet, ExecutionContext executionContext) {
      this.resultStream = resultStream;
      this.resultSet = resultSet;
      this.executionContext = executionContext;
    }

    /**
     * When the last SingleResult is returned, it will synchronized the hasReadAllQueryResults flag of VerdictStreamResultSet
     * to block possible call of next(). It will set status of VerdictStreamResultSet to be complete and
     * append the last SingleResult to VerdictStreamResultSet.
     */
    public void run() {
      while (!resultStream.isCompleted()) {
        VerdictSingleResult singleResult = resultStream.next();
        if (!resultStream.hasNext()) {
          // Must have synchronized keyword. Need to make sure the block is atomic because VerdictStreamResultSet.next() also use hasReadAllQueryResults flag.
          // Otherwise, VerdictStreamResultSet may be unware the processing is completed after the last singleResult is appended.
          synchronized ((Object) resultSet.hasReadAllQueryResults) {
            resultSet.appendSingleResult(singleResult);
            resultSet.setCompleted();
          }
          log.debug("Execution Completed\n");
        } else {
          resultSet.appendSingleResult(singleResult);
        }
      }
    }

    public void abort() {
      executionContext.abort();
    }
  }

  private Boolean checkStreamQuery(String query) {
    if (query.trim().toLowerCase().startsWith("stream")) {
      return true;
    }
    return false;
  }

  @Override
  public boolean execute(String sql) throws SQLException {
    try {
      result = executionContext.sql(sql, false);
      if (result == null) {
        return false;
      }
      return !result.isEmpty();
    } catch (VerdictDBException e) {
      throw new SQLException(e);
    }
  }

  @Override
  public ResultSet executeQuery(String sql) throws SQLException {
    try {
      if (checkStreamQuery(sql)) {
        VerdictStreamResultSet resultSet = new VerdictStreamResultSet();
        sql = sql.replaceFirst("(?i)stream", "");
        VerdictResultStream resultStream = executionContext.streamsql(sql);
        ExecuteStream executeStream = new ExecuteStream(resultStream, resultSet, executionContext);
        resultSet.setRunnable(executeStream);
        new Thread(executeStream).start();
        return resultSet;
      }
      result = executionContext.sql(sql);
      return new VerdictResultSet(result);
    } catch (VerdictDBException e) {
      throw new SQLException(e);
    }
  }

  @Override
  public int executeUpdate(String sql) throws SQLException {
    try {
      result = executionContext.sql(sql);
      return (int) result.getRowCount();
    } catch (VerdictDBException e) {
      throw new SQLException(e);
    }
  }

  @Override
  public void close() throws SQLException {
    // dongyoungy: is this correct for close() to also call terminate() just like cancel()?
    executionContext.terminate();
  }

  @Override
  public boolean isClosed() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public ResultSet getResultSet() throws SQLException {
    return new VerdictResultSet(result);
  }

  @Override
  public java.sql.Connection getConnection() throws SQLException {
    return conn;
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getMaxFieldSize() throws SQLException {
    return 0; // no limit
  }

  @Override
  public void setMaxFieldSize(int max) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getMaxRows() throws SQLException {
    return 0; // no limit
  }

  @Override
  public void setMaxRows(int max) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setEscapeProcessing(boolean enable) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getQueryTimeout() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setQueryTimeout(int seconds) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void cancel() throws SQLException {
    executionContext.terminate();
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    return null;
  }

  @Override
  public void clearWarnings() throws SQLException {
  }

  @Override
  public void setCursorName(String name) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getUpdateCount() throws SQLException {
    return 0;
  }

  @Override
  public boolean getMoreResults() throws SQLException {
    return false;
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getFetchDirection() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getFetchSize() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getResultSetConcurrency() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getResultSetType() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void addBatch(String sql) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void clearBatch() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int[] executeBatch() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean getMoreResults(int current) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public ResultSet getGeneratedKeys() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int executeUpdate(String sql, String[] columnNames) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean execute(String sql, int[] columnIndexes) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean execute(String sql, String[] columnNames) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getResultSetHoldability() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setPoolable(boolean poolable) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean isPoolable() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void closeOnCompletion() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean isCloseOnCompletion() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }
}
