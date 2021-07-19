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

import java.util.ArrayList;
import java.util.List;

import org.verdictdb.exception.VerdictDBValueException;
import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.core.sqlobject.AbstractRelation;
import org.verdictdb.core.sqlobject.AliasedColumn;
import org.verdictdb.core.sqlobject.AsteriskColumn;
import org.verdictdb.core.sqlobject.BaseColumn;
import org.verdictdb.core.sqlobject.BaseTable;
import org.verdictdb.core.sqlobject.ColumnOp;
import org.verdictdb.core.sqlobject.JoinTable;
import org.verdictdb.core.sqlobject.SelectItem;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.core.sqlobject.SubqueryColumn;
import org.verdictdb.core.sqlobject.UnnamedColumn;

public class SubqueriesToDependentNodes {

  /**
   * @param query A query that may include subqueries. The subqueries of this query will be replaced
   *     by placeholders.
   * @param node
   * @throws VerdictDBValueException
   */
  public static void convertSubqueriesToDependentNodes(
          SelectQuery query, CreateTableAsSelectNode node) {
    IdCreator namer = node.getNamer();

    // from list
    for (AbstractRelation source : query.getFromList()) {
      int index = query.getFromList().indexOf(source);

      // If the table is subquery, we need to add it to dependency
      if (source instanceof SelectQuery) {
        CreateTableAsSelectNode dep;
        if (source.isSupportedAggregate()) {
          dep = AggExecutionNode.create(namer, (SelectQuery) source);
        } else {
          dep = ProjectionNode.create(namer, (SelectQuery) source);
        }
        //        node.addDependency(dep);

        // use placeholders to mark the locations whose names will be updated in the future
        Pair<BaseTable, SubscriptionTicket> baseAndSubscriptionTicket =
            node.createPlaceHolderTable(source.getAliasName().get());
        query.getFromList().set(index, baseAndSubscriptionTicket.getLeft());
        dep.registerSubscriber(baseAndSubscriptionTicket.getRight());
        //        dep.addBroadcastingQueue(baseAndQueue.getRight());
      } else if (source instanceof JoinTable) {
        for (AbstractRelation s : ((JoinTable) source).getJoinList()) {
          int joinindex = ((JoinTable) source).getJoinList().indexOf(s);

          // If the table is subquery, we need to add it to dependency
          if (s instanceof SelectQuery) {
            CreateTableAsSelectNode dep;
            if (s.isSupportedAggregate()) {
              dep = AggExecutionNode.create(namer, (SelectQuery) s);
            } else {
              dep = ProjectionNode.create(namer, (SelectQuery) s);
            }
            //            node.addDependency(dep);

            // use placeholders to mark the locations whose names will be updated in the future
            Pair<BaseTable, SubscriptionTicket> baseAndSubscriptionTicket =
                node.createPlaceHolderTable(s.getAliasName().get());
            ((JoinTable) source).getJoinList().set(joinindex, baseAndSubscriptionTicket.getLeft());
            dep.registerSubscriber(baseAndSubscriptionTicket.getRight());
            //            dep.addBroadcastingQueue(baseAndQueue.getRight());
          }
        }
      }
    }

    //    int filterPlaceholderNum = 0;
    // Filter
    if (query.getFilter().isPresent()) {
      UnnamedColumn where = query.getFilter().get();
      List<UnnamedColumn> filters = new ArrayList<>();
      filters.add(where);
      while (!filters.isEmpty()) {
        UnnamedColumn filter = filters.get(0);
        filters.remove(0);

        // If filter is a subquery, we need to add it to dependency
        if (filter instanceof SubqueryColumn) {
          Pair<BaseTable, SubscriptionTicket> baseAndSubscriptionTicket;
          if (((SubqueryColumn) filter).getSubquery().getAliasName().isPresent()) {
            baseAndSubscriptionTicket =
                node.createPlaceHolderTable(
                    ((SubqueryColumn) filter).getSubquery().getAliasName().get());
          } else {
            //            baseAndQueue =
            // node.createPlaceHolderTable("filterPlaceholder"+filterPlaceholderNum++);
            baseAndSubscriptionTicket = node.createPlaceHolderTable(namer.generateAliasName());
          }
          BaseTable base = baseAndSubscriptionTicket.getLeft();

          CreateTableAsSelectNode dep;
          SelectQuery subquery = ((SubqueryColumn) filter).getSubquery();
          if (subquery.isSupportedAggregate()) {
            dep = AggExecutionNode.create(namer, subquery);
            //            node.addDependency(dep);
          } else {
            dep = ProjectionNode.create(namer, subquery);
            //            node.addDependency(dep);
          }
          dep.registerSubscriber(baseAndSubscriptionTicket.getRight());
          //          dep.addBroadcastingQueue(baseAndQueue.getRight());

          // To replace the subquery, we use the selectlist of the subquery and tempTable to create
          // a new non-aggregate subquery
          List<SelectItem> newSelectItem = new ArrayList<>();
          for (SelectItem item : subquery.getSelectList()) {
            if (item instanceof AliasedColumn) {
              newSelectItem.add(
                  new AliasedColumn(
                      new BaseColumn(
                          base.getSchemaName(),
                          base.getAliasName().get(),
                          ((AliasedColumn) item).getAliasName()),
                      ((AliasedColumn) item).getAliasName()));
            } else if (item instanceof AsteriskColumn) {
              newSelectItem.add(new AsteriskColumn());
            }
          }
          SelectQuery newSubquery = SelectQuery.create(newSelectItem, base);
          if (((SubqueryColumn) filter).getSubquery().getAliasName().isPresent()) {
            newSubquery.setAliasName(((SubqueryColumn) filter).getSubquery().getAliasName().get());
          }
          ((SubqueryColumn) filter).setSubquery(newSubquery);
          node.getPlaceholderTablesinFilter().add((SubqueryColumn) filter);
        } else if (filter instanceof ColumnOp) {
          filters.addAll(((ColumnOp) filter).getOperands());
        }
      }
    }
  }
}
