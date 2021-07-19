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

package org.verdictdb.core.execplan;

import org.verdictdb.commons.VerdictDBLogger;
import org.verdictdb.core.querying.ExecutableNodeBase;
import org.verdictdb.core.querying.ola.AsyncAggExecutionNode;
import org.verdictdb.core.querying.ola.SelectAsyncAggExecutionNode;
import org.verdictdb.core.querying.simplifier.ConsolidatedExecutionNode;
import org.verdictdb.core.sqlobject.SqlConvertible;
import org.verdictdb.exception.VerdictDBDbmsException;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.exception.VerdictDBValueException;
import org.verdictdb.sqlsyntax.MysqlSyntax;
import org.verdictdb.sqlwriter.QueryToSql;
import org.verdictdb.connection.CachedDbmsConnection;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.connection.JdbcConnection;
import org.verdictdb.connection.SparkConnection;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class ExecutableNodeRunner implements Runnable {

  DbmsConnection conn;

  ExecutableNode node;

  int successSourceCount = 0;

  int dependentCount;

  //  private boolean isAborted = false;

  /**
   * initiated: node is created but has not started running running: currently running aborted: node
   * running cancelled while running cancelled: nod running cancelled before running completed: node
   * running successfully finished
   */
  enum NodeRunningStatus {
    initiated,
    running,
    aborted,
    cancelled,
    completed,
    failed;
  }

  private NodeRunningStatus status = NodeRunningStatus.initiated;

  private VerdictDBLogger log = VerdictDBLogger.getLogger(this.getClass());

  private Thread runningTask = null;

  private List<ExecutableNodeRunner> childRunners = new ArrayList<>();

  public void markComplete() {
    status = NodeRunningStatus.completed;
    clearRunningTask();
  }

  public void markFailure() {
    status = NodeRunningStatus.failed;
    clearRunningTask();
  }

  public void markInitiated() {
    status = NodeRunningStatus.initiated;
    clearRunningTask();
  }

  private void clearRunningTask() {
    this.runningTask = null;
  }

  public ExecutableNodeRunner(DbmsConnection conn, ExecutableNode node) {
    this.conn = conn;
    node.registerNodeRunner(this);
    this.node = node;
    this.dependentCount = node.getDependentNodeCount();
  }

  public static ExecutionInfoToken execute(DbmsConnection conn, ExecutableNode node)
      throws VerdictDBException {
    return execute(conn, node, Arrays.<ExecutionInfoToken>asList());
  }

  public static ExecutionInfoToken execute(
      DbmsConnection conn, ExecutableNode node, List<ExecutionInfoToken> tokens)
      throws VerdictDBException {
    return (new ExecutableNodeRunner(conn, node)).execute(tokens);
  }

  public NodeRunningStatus getStatus() {
    return status;
  }

  /** Set aborted to the status of this node. */
  public void setAborted() {
    //    isAborted = true;   // this will effectively end the loop within run().
    status = NodeRunningStatus.aborted;
    //    for (ExecutableNodeRunner runner : ((ExecutableNodeBase) node) ) {
    //      runner.setAborted();
    //    }
  }

  public boolean alreadyRunning() {
    return status == NodeRunningStatus.running;
  }

  public boolean noNeedToRun() {
    return status == NodeRunningStatus.aborted
        || status == NodeRunningStatus.cancelled
        || status == NodeRunningStatus.completed
        || status == NodeRunningStatus.failed;
  }

  /** Aborts this node. */
  public void abort() {
    log.trace(String.format("Aborts running this node %s", node.toString()));
    setAborted();
    if (node instanceof SelectAsyncAggExecutionNode) {
      ((SelectAsyncAggExecutionNode) node).abort();
    }
    conn.abort();
    //    for (ExecutableNodeRunner runner : childRunners) {
    //      runner.abort();
    //    }
  }

  public boolean runThisAndDependents() {
    // first run all children on separate threads
    // this function may be called again when run() is triggered upon a completion of one of
    // child nodes. Therefore, runChildren() is responsible for ensuring the same node does not
    // run again.
    runDependents();
    return runOnThread();
  }

  private boolean doesThisNodeContainAsyncAggExecutionNode() {
    ExecutableNodeBase leafOfThis = (ExecutableNodeBase) node;
    while (leafOfThis instanceof ConsolidatedExecutionNode) {
      leafOfThis = ((ConsolidatedExecutionNode) leafOfThis).getChildNode();
    }
    return leafOfThis instanceof AsyncAggExecutionNode;
  }

  public boolean runOnThread() {
    log.trace(String.format("Invoked to run: %s", node.toString()));

    // https://stackoverflow.com/questions/11165852/java-singleton-and-synchronization
    Thread runningTask = this.runningTask;
    if (runningTask == null) {
      synchronized (this) {
        runningTask = this.runningTask;

        if (runningTask == null) {
          if (noNeedToRun()) {
            log.trace(String.format("No need to run: %s", node.toString()));
            return false;
          }
          status = NodeRunningStatus.running;

          runningTask = new Thread(this);
          this.runningTask = runningTask;
          runningTask.start();

          return true;
          // this.runningTask is set to null at the end of run()
        }
      }
    }

    return false;
  }

  private int getMaxNumberOfRunningNode() {
    if ((conn instanceof JdbcConnection && conn.getSyntax() instanceof MysqlSyntax)
        || (conn instanceof CachedDbmsConnection
                && ((CachedDbmsConnection) conn).getOriginalConnection() instanceof JdbcConnection)
            && ((CachedDbmsConnection) conn).getOriginalConnection().getSyntax()
                instanceof MysqlSyntax) {
      // For MySQL, issue query one by one.
      if (node instanceof SelectAsyncAggExecutionNode || node instanceof AsyncAggExecutionNode) {
        return 1;
      } else {
        return 1;
      }
    } else if (((conn instanceof SparkConnection)
            || (conn instanceof CachedDbmsConnection
                && ((CachedDbmsConnection) conn).getOriginalConnection()
                    instanceof SparkConnection))
        && !(node instanceof SelectAsyncAggExecutionNode)) {
      // Since abort() does not work for Spark (or I don't know how to do so), we issue query
      // one by one.
      return 1;
    } else {
      return 10;
    }
  }

  private void runDependents() {
    if (doesThisNodeContainAsyncAggExecutionNode()) {
      int maxNumberOfRunningNode = getMaxNumberOfRunningNode();
      int currentlyRunningOrCompleteNodeCount = childRunners.size();

      // check the number of currently running nodes
      int runningChildCount = 0;
      int completedChildCount = 0;
      for (ExecutableNodeRunner r : childRunners) {
        if (r.getStatus() == NodeRunningStatus.running) {
          runningChildCount++;
        } else if (r.getStatus() == NodeRunningStatus.completed) {
          completedChildCount++;
        }
      }
      log.trace(
          String.format(
              "Running child: %d, Completed Child: %d, Success token received: %d",
              runningChildCount, completedChildCount, successSourceCount));

      // maintain the number of running nodes to a certain number
      List<ExecutableNodeBase> childNodes = ((ExecutableNodeBase) node).getSources();
      int moreToRun =
          Math.min(
              maxNumberOfRunningNode - runningChildCount,
              ((ExecutableNodeBase) node).getSourceCount() - currentlyRunningOrCompleteNodeCount);
      for (int i = currentlyRunningOrCompleteNodeCount;
          i < currentlyRunningOrCompleteNodeCount + moreToRun;
          i++) {
        ExecutableNodeBase child = childNodes.get(i);

        ExecutableNodeRunner runner = child.getRegisteredRunner();
        boolean started = runner.runThisAndDependents();
        if (started) {
          childRunners.add(runner);
        }
      }
    } else {
      // by default, run every child
      for (ExecutableNodeBase child : ((ExecutableNodeBase) node).getSources()) {
        ExecutableNodeRunner runner = child.getRegisteredRunner();
        boolean started = runner.runThisAndDependents();
        if (started) {
          childRunners.add(runner);
        }
      }
    }
  }

  /** A single run of this method consumes all combinations of the tokens in the queue. */
  @Override
  public synchronized void run() {
    //    String nodeType = node.getClass().getSimpleName();
    //    int nodeGroupId = ((ExecutableNodeBase) node).getGroupId();

    if (noNeedToRun()) {
      log.debug(String.format("This node (%s) has been aborted; do not run.", node.toString()));
      clearRunningTask();
      return;
    }

    // no dependency exists
    if (node.getSourceQueues().size() == 0) {
      log.trace(String.format("No dependency exists. Simply run %s", node.toString()));
      try {
        executeAndBroadcast(Arrays.<ExecutionInfoToken>asList());
        broadcastAndTriggerRun(ExecutionInfoToken.successToken());
        markComplete();
        // clearRunningTask();
        return;
      } catch (Exception e) {
        if (noNeedToRun()) {
          // do nothing
          return;
        } else {
          e.printStackTrace();
          broadcastAndTriggerRun(ExecutionInfoToken.failureToken(e));
        }
      }
      markFailure();
      //      clearRunningTask();
      return;
    }

    // dependency exists
    while (!noNeedToRun()) {
      // not enough source nodes are finished (i.e., tokens = null)
      // then, this loop immediately terminates.
      // this function will be called again whenever a child node completes.
      List<ExecutionInfoToken> tokens = retrieve();
      if (tokens == null) {
        log.trace("Not enough source nodes are finished, loop terminates");
        //        markInitiated();
        clearRunningTask();
        return;
      }

      log.trace(String.format("Attempts to process %s (%s)", node.toString(), status));

      ExecutionInfoToken failureToken = getFailureTokenIfExists(tokens);
      if (failureToken != null) {
        log.trace(String.format("One or more dependent nodes failed for %s", node.toString()));
        broadcastAndTriggerRun(failureToken);
        //        clearRunningTask();
        markFailure();
        return;
      }
      if (areAllSuccess(tokens)) {
        log.trace(String.format("All dependent nodes are finished for %s", node.toString()));
        broadcastAndTriggerRun(ExecutionInfoToken.successToken());
        // clearRunningTask();
        markComplete();
        return;
      }

      // This happens because some of the children (e.g., individual agg blocks)
      // finished their processing (with parts of blocks).
      // Since there still are other children to process, we continue operation.
      if (areAllStatusTokens(tokens)) {
        continue;
      }

      // actual processing
      try {
        log.trace(
            String.format("Main processing starts for %s with token: %s", node.toString(), tokens));
        executeAndBroadcast(tokens);
      } catch (Exception e) {
        if (noNeedToRun()) {
          // do nothing
          return;
        } else {
          e.printStackTrace();
          broadcastAndTriggerRun(ExecutionInfoToken.failureToken(e));
          markFailure();
          //          clearRunningTask();
          return;
        }
      }
    }
  }

  synchronized List<ExecutionInfoToken> retrieve() {
    Map<Integer, ExecutionTokenQueue> sourceChannelAndQueues = node.getSourceQueues();

    for (ExecutionTokenQueue queue : sourceChannelAndQueues.values()) {
      ExecutionInfoToken rs = queue.peek();
      if (rs == null) {
        return null;
      }
    }

    // all results available now
    List<ExecutionInfoToken> results = new ArrayList<>();
    for (Entry<Integer, ExecutionTokenQueue> channelAndQueue : sourceChannelAndQueues.entrySet()) {
      int channel = channelAndQueue.getKey();
      ExecutionInfoToken rs = channelAndQueue.getValue().take();
      rs.setKeyValue("channel", channel);
      results.add(rs);
    }

    return results;
  }

  void broadcastAndTriggerRun(ExecutionInfoToken token) {
    if (noNeedToRun() && !areAllStatusTokens(Arrays.asList(token))) {
      log.trace(
          String.format(
              "This node (%s) has been aborted. Do not broadcast: %s", this.toString(), token));
      return;
    }

    // for understandable logs
    synchronized (VerdictDBLogger.class) {
      VerdictDBLogger logger = VerdictDBLogger.getLogger(this.getClass());
      logger.trace(String.format("[%s] Broadcasting:", node.toString()));
      logger.trace(token.toString());
      for (ExecutableNode dest : node.getSubscribers()) {
        logger.trace(String.format("  -> %s", dest.toString()));
      }
      //      logger.trace(token.toString());
    }

    // if (node instanceof SelectAggExecutionNode && areAllStatusTokens(Arrays.asList(token))) {
    //  status = NodeRunningStatus.completed;
    // }

    for (ExecutableNode dest : node.getSubscribers()) {
      ExecutionInfoToken copiedToken = token.deepcopy();
      // set runner with success token complete before notifying subscriber
      if (copiedToken.isSuccessToken()
          && node.getRegisteredRunner().getStatus() != NodeRunningStatus.completed) {
        node.getRegisteredRunner().status = NodeRunningStatus.completed;
      }
      dest.getNotified(node, copiedToken);

      // signal the runner of the broadcasted node so that its associated runner performs
      // execution if necessary.
      ExecutableNodeRunner runner = dest.getRegisteredRunner();
      if (runner != null) {
        //        runner.runOnThread();
        log.trace("Broadcast: status " + status);
        runner.runThisAndDependents(); // this is needed due to async node.
      }
    }
  }

  void executeAndBroadcast(List<ExecutionInfoToken> tokens) throws VerdictDBException {
    ExecutionInfoToken resultToken = execute(tokens);
    if (resultToken != null) {
      broadcastAndTriggerRun(resultToken);
    }
  }

  /**
   * Execute the associated node. This method is synchronized on object level, assuming that every
   * node has its own associated ExecutableNodeRunner, which is actually the case.
   *
   * @param tokens Contains information from the downstream nodes.
   * @return Information for the upstream nodes.
   * @throws VerdictDBException
   */
  public ExecutionInfoToken execute(List<ExecutionInfoToken> tokens) throws VerdictDBException {
    if (tokens.size() > 0 && tokens.get(0).isStatusToken()) {
      return null;
    }

    // basic operations: execute a query and creates a token based on that result.
    SqlConvertible sqlObj = node.createQuery(tokens);
    DbmsQueryResult intermediate = null;
    if (sqlObj != null) {
      String sql = QueryToSql.convert(conn.getSyntax(), sqlObj);
      try {
        intermediate = conn.execute(sql);
      } catch (VerdictDBDbmsException e) {
        if (noNeedToRun()) {
          // the errors from the underlying dbms are expected if the query is cancelled.
        } else {
          throw e;
        }
      }
    }
    ExecutionInfoToken token = node.createToken(intermediate);

    // extended operations: if the node has additional method invocation list, we perform the method
    // calls
    // on DbmsConnection and sets its results in the token.
    if (token == null) {
      token = new ExecutionInfoToken();
    }
    Map<String, MethodInvocationInformation> tokenKeysAndmethodsToInvoke =
        node.getMethodsToInvokeOnConnection();
    for (Entry<String, MethodInvocationInformation> keyAndMethod :
        tokenKeysAndmethodsToInvoke.entrySet()) {
      String tokenKey = keyAndMethod.getKey();
      MethodInvocationInformation methodInfo = keyAndMethod.getValue();
      String methodName = methodInfo.getMethodName();
      Class<?>[] methodParameters = methodInfo.getMethodParameters();
      Object[] methodArguments = methodInfo.getArguments();

      try {
        Method method = conn.getClass().getMethod(methodName, methodParameters);
        Object ret = method.invoke(conn, methodArguments);
        token.setKeyValue(tokenKey, ret);

      } catch (NoSuchMethodException
          | IllegalAccessException
          | IllegalArgumentException
          | InvocationTargetException e) {
        e.printStackTrace();
        throw new VerdictDBValueException(e);
      }
    }

    return token;
  }

  ExecutionInfoToken getFailureTokenIfExists(List<ExecutionInfoToken> tokens) {
    for (ExecutionInfoToken t : tokens) {
      //      System.out.println(t);
      if (t.isFailureToken()) {
        //        System.out.println("yes");
        return t;
      }
    }
    return null;
  }

  boolean areAllStatusTokens(List<ExecutionInfoToken> tokens) {
    for (ExecutionInfoToken t : tokens) {
      if (!t.isStatusToken()) {
        return false;
      }
    }
    return true;
  }

  boolean areAllSuccess(List<ExecutionInfoToken> tokens) {
    for (ExecutionInfoToken t : tokens) {
      if (t.isSuccessToken()) {
        synchronized ((Object) successSourceCount) {
          successSourceCount++;
        }
        log.trace(String.format("Success count of %s: %d", node.toString(), successSourceCount));
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
