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

package org.verdictdb.core.querying;

import java.util.List;

import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.core.sqlobject.SqlConvertible;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.core.execplan.ExecutionInfoToken;

public class ProjectionNode extends CreateTableAsSelectNode {

  private static final long serialVersionUID = 3168447303333633662L;

  public ProjectionNode(IdCreator namer, SelectQuery query) {
    super(namer, query);
  }

  public static ProjectionNode create(IdCreator namer, SelectQuery query) {
    ProjectionNode node = new ProjectionNode(namer, null);
    SubqueriesToDependentNodes.convertSubqueriesToDependentNodes(query, node);
    node.setSelectQuery(query);
    return node;
  }

  @Override
  public SqlConvertible createQuery(List<ExecutionInfoToken> tokens) throws VerdictDBException {
    return super.createQuery(tokens);
  }

  @Override
  public ExecutionInfoToken createToken(DbmsQueryResult result) {
    return super.createToken(result);
  }

  @Override
  public ExecutableNodeBase deepcopy() {
    ProjectionNode node = new ProjectionNode(namer, selectQuery);
    copyFields(this, node);
    return node;
  }
}
