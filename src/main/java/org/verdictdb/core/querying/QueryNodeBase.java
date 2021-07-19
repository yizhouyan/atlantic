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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.core.sqlobject.SqlConvertible;
import org.verdictdb.exception.VerdictDBException;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.verdictdb.core.execplan.ExecutableNode;
import org.verdictdb.core.execplan.ExecutionInfoToken;

public class QueryNodeBase extends ExecutableNodeBase {

  private static final long serialVersionUID = 7263437396821994391L;

  protected SelectQuery selectQuery;
  
  private Map<Long, Map<String, Object>> threadSafeStorage = new HashMap<>();

  public QueryNodeBase(IdCreator idCreator, SelectQuery selectQuery) {
    super(idCreator);
    this.selectQuery = selectQuery;
  }
  
  public QueryNodeBase(int uniqueId, SelectQuery selectQuery) {
    super(uniqueId);
    this.selectQuery = selectQuery;
  }

  List<ExecutableNode> getParents() {
    return getSubscribers();
  }

  public SelectQuery getSelectQuery() {
    return selectQuery;
  }

  public void setSelectQuery(SelectQuery query) {
    this.selectQuery = query;
  }

  @Override
  public SqlConvertible createQuery(List<ExecutionInfoToken> tokens) throws VerdictDBException {
    return selectQuery;
  }

  @Override
  public ExecutionInfoToken createToken(DbmsQueryResult result) {
    ExecutionInfoToken token = new ExecutionInfoToken();
    token.setKeyValue("queryResult", result);
    return token;
  }

  @Override
  public ExecutableNodeBase deepcopy() {
    QueryNodeBase node = new QueryNodeBase(uniqueId, selectQuery);
    copyFields(this, node);
    return node;
  }

  protected void copyFields(QueryNodeBase from, QueryNodeBase to) {
    super.copyFields(from, to);
    to.selectQuery = from.selectQuery.deepcopy();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (obj == this) {
      return true;
    }
    if (obj.getClass() != getClass()) {
      return false;
    }
    QueryNodeBase rhs = (QueryNodeBase) obj;
    return new EqualsBuilder()
        .appendSuper(super.equals(obj))
        .append(selectQuery, rhs.selectQuery)
        .isEquals();
  }
}
