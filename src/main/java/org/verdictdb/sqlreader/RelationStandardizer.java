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

package org.verdictdb.sqlreader;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Vector;

import org.verdictdb.connection.MetaDataProvider;
import org.verdictdb.sqlsyntax.H2Syntax;
import org.verdictdb.sqlsyntax.SqlSyntax;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.core.sqlobject.AbstractRelation;
import org.verdictdb.core.sqlobject.AliasReference;
import org.verdictdb.core.sqlobject.AliasedColumn;
import org.verdictdb.core.sqlobject.AsteriskColumn;
import org.verdictdb.core.sqlobject.BaseColumn;
import org.verdictdb.core.sqlobject.BaseTable;
import org.verdictdb.core.sqlobject.ColumnOp;
import org.verdictdb.core.sqlobject.ConstantColumn;
import org.verdictdb.core.sqlobject.GroupingAttribute;
import org.verdictdb.core.sqlobject.JoinTable;
import org.verdictdb.core.sqlobject.OrderbyAttribute;
import org.verdictdb.core.sqlobject.SelectItem;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.core.sqlobject.SubqueryColumn;
import org.verdictdb.core.sqlobject.UnnamedColumn;
import org.verdictdb.exception.VerdictDBDbmsException;
import org.verdictdb.exception.VerdictDBException;


public class RelationStandardizer {

  private MetaDataProvider meta;

  private SqlSyntax syntax;

  private static long itemID = 1;

  private static long duplicateIdentifer = 1;

  private static String verdictTableAliasPrefix = "vt";

  // key is the column name and value is table alias name
  private HashMap<String, String> colNameAndTableAlias = new HashMap<>();

  // key is the schema name and table name and the value is table alias name
  private HashMap<Pair<String, String>, String> tableInfoAndAlias = new HashMap<>();

  // key is the select column name, value is their alias
  private HashMap<String, String> colNameAndColAlias = new HashMap<>();

  // key is columnOp, value is their alias name
  private HashMap<ColumnOp, String> columnOpAliasMap = new HashMap<>();

  // key is schema name, column name, value is alias
  // only store value if there are duplicate column names
  private HashMap<Pair<String, String>, String> duplicateColNameAndColAlias = new HashMap<>();
  /**
   * * If From list is a subquery, we need to record the column alias name in colNameAndTempColAlias
   * so that we can replace the select item with the column alias name we generate.
   */
  private HashMap<String, String> colNameAndTempColAlias = new HashMap<>();

  // Since we replace all table alias using our generated alias name, this map will record the table
  // alias name we replaced.
  private HashMap<String, String> oldTableAliasMap = new HashMap<>();

  public RelationStandardizer(MetaDataProvider meta) {
    this.meta = meta;
  }

  public RelationStandardizer(MetaDataProvider meta, SqlSyntax syntax) {
    this.meta = meta;
    this.syntax = syntax;
  }
  

  public static SelectQuery standardizeSelectQuery(
      SelectQuery selectQuery, 
      MetaDataProvider metaData,
      SqlSyntax syntax) 
      throws VerdictDBException {
//    MetaDataProvider metaData = createMetaDataFor(selectQuery);
    RelationStandardizer gen = new RelationStandardizer(metaData, syntax);
    selectQuery = gen.standardize(selectQuery);
//    selectQuery.setStandardized();
    return selectQuery;
  }

  /**
   * (optional) set database syntax to enable database-specific standardization.
   *
   * @param syntax
   */
  public void setSyntax(SqlSyntax syntax) {
    this.syntax = syntax;
  }

  private BaseColumn replaceBaseColumn(BaseColumn col) {
    if (col.getTableSourceAlias().equals("")) {
      if (!(col.getSchemaName().equals(""))) {
        col.setTableSourceAlias(
            tableInfoAndAlias.get(new ImmutablePair<>(col.getSchemaName(), col.getTableName())));
      } else {
        col.setTableSourceAlias(colNameAndTableAlias.get(col.getColumnName()));
      }
    }
    if (colNameAndTempColAlias.containsKey(col.getColumnName())) {
      col.setColumnName(colNameAndTempColAlias.get(col.getColumnName()));
    }
    if (oldTableAliasMap.containsKey(col.getTableSourceAlias())) {
      col.setTableSourceAlias(oldTableAliasMap.get(col.getTableSourceAlias()));
    }
    
    // Yongjoo: I unsurely commented out these lines, believing that keeping only alias names
    //          will be enough.
//    if (tableInfoAndAlias.containsValue(col.getTableSourceAlias())) {
//      for (Map.Entry<Pair<String, String>, String> entry : tableInfoAndAlias.entrySet()) {
//        if (entry.getValue().equals(col.getTableSourceAlias())) {
//          col.setSchemaName(entry.getKey().getLeft());
//          col.setTableName(entry.getKey().getRight());
//          break;
//        }
//      }
//    }
    
    // When no schema name is specified, the table name of a column is parsed as an alias 
    // by default; however, the alias name can actually be the name of the table.
    if (col.getSchemaName().equals("")) {
      // Yongjoo: I don't think setting the schema is always safe 
      // (e.g., a column is from a subquery)
//      col.setSchemaName(meta.getDefaultSchema());
      String defaultSchema = meta.getDefaultSchema();
      Pair<String, String> searchKey = 
          new ImmutablePair<>(defaultSchema, col.getTableSourceAlias());
      if (tableInfoAndAlias.containsKey(searchKey)) {
        col.setTableSourceAlias(tableInfoAndAlias.get(searchKey));
      }
      
    }

    return col;
  }

  private List<SelectItem> replaceSelectList(List<SelectItem> selectItemList)
      throws VerdictDBDbmsException {
    List<SelectItem> newSelectItemList = new ArrayList<>();
    for (SelectItem sel : selectItemList) {
      if (!(sel instanceof AliasedColumn) && !(sel instanceof AsteriskColumn)) {
        if (sel instanceof BaseColumn) {
          sel = replaceBaseColumn((BaseColumn) sel);
          if (!colNameAndColAlias.containsValue(((BaseColumn) sel).getColumnName())) {
            colNameAndColAlias.put(
                ((BaseColumn) sel).getColumnName(), ((BaseColumn) sel).getColumnName());
            newSelectItemList.add(
                new AliasedColumn((BaseColumn) sel, ((BaseColumn) sel).getColumnName()));
          } else {
            duplicateColNameAndColAlias.put(
                new ImmutablePair<>(
                    ((BaseColumn) sel).getTableSourceAlias(), ((BaseColumn) sel).getColumnName()),
                ((BaseColumn) sel).getColumnName() + duplicateIdentifer);
            newSelectItemList.add(
                new AliasedColumn(
                    (BaseColumn) sel, ((BaseColumn) sel).getColumnName() + duplicateIdentifer++));
          }
        } else if (sel instanceof ColumnOp) {
          // First replace the possible base column inside the columnop using the same way we did on
          // Where clause
          sel = replaceFilter((ColumnOp) sel);

          if (((ColumnOp) sel).getOpType().equals("count")) {
            columnOpAliasMap.put((ColumnOp) sel, "c" + itemID);
            newSelectItemList.add(new AliasedColumn((ColumnOp) sel, "c" + itemID++));
          } else if (((ColumnOp) sel).getOpType().equals("sum")) {
            columnOpAliasMap.put((ColumnOp) sel, "s" + itemID);
            newSelectItemList.add(new AliasedColumn((ColumnOp) sel, "s" + itemID++));
          } else if (((ColumnOp) sel).getOpType().equals("avg")) {
            columnOpAliasMap.put((ColumnOp) sel, "a" + itemID);
            newSelectItemList.add(new AliasedColumn((ColumnOp) sel, "a" + itemID++));
          } else if (((ColumnOp) sel).getOpType().equals("countdistinct")) {
            columnOpAliasMap.put((ColumnOp) sel, "cd" + itemID);
            newSelectItemList.add(new AliasedColumn((ColumnOp) sel, "cd" + itemID++));
          } else {
            columnOpAliasMap.put((ColumnOp) sel, "vc" + itemID);
            newSelectItemList.add(new AliasedColumn((ColumnOp) sel, "vc" + itemID++));
          }
        }
      } else {
        if (sel instanceof AliasedColumn) {
          ((AliasedColumn) sel).setColumn(replaceFilter(((AliasedColumn) sel).getColumn()));
        }
        newSelectItemList.add(sel);
        if (sel instanceof AliasedColumn
            && ((AliasedColumn) sel).getColumn() instanceof BaseColumn) {
          colNameAndColAlias.put(
              ((BaseColumn) ((AliasedColumn) sel).getColumn()).getColumnName(),
              ((AliasedColumn) sel).getAliasName());
        } else if (sel instanceof AliasedColumn
            && ((AliasedColumn) sel).getColumn() instanceof ColumnOp) {
          columnOpAliasMap.put(
              ((ColumnOp) ((AliasedColumn) sel).getColumn()), ((AliasedColumn) sel).getAliasName());
        }
      }
    }
    return newSelectItemList;
  }

  // Use BFS to search all the condition.
  private UnnamedColumn replaceFilter(UnnamedColumn condition) throws VerdictDBDbmsException {
    List<UnnamedColumn> searchList = new Vector<>();
    searchList.add(condition);
    while (!searchList.isEmpty()) {
      UnnamedColumn cond = searchList.get(0);
      searchList.remove(0);
      if (cond instanceof BaseColumn) {
        cond = replaceBaseColumn((BaseColumn) cond);
      } else if (cond instanceof ColumnOp) {
        for (UnnamedColumn col : ((ColumnOp) cond).getOperands()) {
          searchList.add(col);
        }
      } else if (cond instanceof SubqueryColumn) {
        RelationStandardizer g = new RelationStandardizer(meta, syntax);
        g.oldTableAliasMap.putAll(oldTableAliasMap);
        g.setColNameAndColAlias(colNameAndColAlias);
        g.setColumnOpAliasMap(columnOpAliasMap);
        g.setColNameAndTableAlias(colNameAndTableAlias);
        g.setTableInfoAndAlias(tableInfoAndAlias);
        SelectQuery newSubquery = g.standardize(((SubqueryColumn) cond).getSubquery());
        ((SubqueryColumn) cond).setSubquery(newSubquery);
      }
    }
    return condition;
  }

  private AliasedColumn matchAliasFromSelectList(List<SelectItem> selectItems, BaseColumn col) {
    for (SelectItem item : selectItems) {
      if (item instanceof AliasedColumn) {
        AliasedColumn aliasedColumn = (AliasedColumn) item;
        if (aliasedColumn.getAliasName().equals(col.getColumnName())) {
          return aliasedColumn;
        }
      }
    }
    return null;
  }

  private List<GroupingAttribute> replaceGroupby(
      List<SelectItem> selectItems, List<GroupingAttribute> groupingAttributeList)
      throws VerdictDBDbmsException {
    return this.replaceGroupby(selectItems, groupingAttributeList, false);
  }

  /**
   * @return: replaced Groupby list or Orderby list If it is groupby, we should return column
   *     instead of alias
   */
  private List<GroupingAttribute> replaceGroupby(
      List<SelectItem> selectItems,
      List<GroupingAttribute> groupingAttributeList,
      boolean isForOrderBy)
      throws VerdictDBDbmsException {
    List<GroupingAttribute> newGroupby = new ArrayList<>();
    for (GroupingAttribute g : groupingAttributeList) {
      if (g instanceof BaseColumn) {
        // 'col' can be either a base column or an alias to a select item
        BaseColumn col = (BaseColumn) g;

        // Check for aliases
        AliasedColumn aliasMatch = matchAliasFromSelectList(selectItems, col);
        if (aliasMatch != null && !isForOrderBy) {
          UnnamedColumn column = aliasMatch.getColumn();
          // Unless it is a subquery (I think it would not be possible, but just in case),
          // we use the actual operation in the group-by.
          if (column instanceof SubqueryColumn) {
            newGroupby.add(new AliasReference(aliasMatch.getAliasName()));
          } else {
            newGroupby.add(column);
          }
        } else if (((BaseColumn) g).getTableSourceAlias() != "") {
          // if it is a base column, let's get its current table alias and replace.
          String tableSource = ((BaseColumn) g).getTableSourceAlias();
          String columnName = ((BaseColumn) g).getColumnName();
          if (duplicateColNameAndColAlias.containsKey(
              new ImmutablePair<>(tableSource, columnName))) {
            newGroupby.add(
                getGroupOrOrderByColumn(
                    tableSource,
                    duplicateColNameAndColAlias.get(new ImmutablePair<>(tableSource, columnName)),
                    isForOrderBy));
          } else if (colNameAndColAlias.containsKey(columnName)) {
            newGroupby.add(
                getGroupOrOrderByColumn(
                    oldTableAliasMap.get(tableSource), columnName, isForOrderBy));
          } else
            newGroupby.add(getGroupOrOrderByColumn(((BaseColumn) g).getColumnName(), isForOrderBy));
        } else
          newGroupby.add(getGroupOrOrderByColumn(((BaseColumn) g).getColumnName(), isForOrderBy));
      } else if (g instanceof ColumnOp) {
        // If it is a column-op, we substitute its table reference to our alias unless
        // this method is called to get order-by columns. In such case, we simply use alias.
        // Also, H2 does not support ColumnOp in group-by.
        if (isForOrderBy || (syntax instanceof H2Syntax)) {
          ColumnOp replaced = (ColumnOp) replaceFilter((ColumnOp) g);
          if (columnOpAliasMap.containsKey(replaced)) {
            newGroupby.add(new AliasReference(columnOpAliasMap.get(replaced)));
          } else newGroupby.add(replaced);
        } else {
          ColumnOp newCol = ((ColumnOp) g).deepcopy();
          this.replaceGroupByReference(newCol);
          newGroupby.add(newCol);
        }
      } else if (g instanceof ConstantColumn) {
        // replace index with column alias
        String value = (String) ((ConstantColumn) g).getValue();
        try {
          Integer.parseInt(value);
        } catch (NumberFormatException e) {
          newGroupby.add(new AliasReference(value));
          continue;
        }
        int index = Integer.valueOf(value);
        AliasedColumn col = (AliasedColumn) selectItems.get(index - 1);
        UnnamedColumn column = col.getColumn();
        if (column instanceof BaseColumn && !isForOrderBy) {
          BaseColumn baseCol = (BaseColumn) column;
          newGroupby.add(new BaseColumn(baseCol.getTableSourceAlias(), baseCol.getColumnName()));
        } else {
          newGroupby.add(new AliasReference(col.getAliasName()));
        }
      }
    }
    return newGroupby;
  }

  // returns BaseColumn for group-by, AliasReference for order-by
  private GroupingAttribute getGroupOrOrderByColumn(
      String table, String column, boolean isForOrderBy) {
    if (isForOrderBy) {
      return (table != null) ? new AliasReference(table, column) : new AliasReference(column);
    }
    else {
      return (table != null) ? new BaseColumn(table, column) : new BaseColumn(column);
    }
  }

  // returns BaseColumn for group-by, AliasReference for order-by
  private GroupingAttribute getGroupOrOrderByColumn(String column, boolean isForOrderBy) {
    if (isForOrderBy) return new AliasReference(column);
    else return new BaseColumn(column);
  }

  private void replaceGroupByReference(UnnamedColumn c) {
    if (c instanceof ColumnOp) {
      ColumnOp colOp = (ColumnOp) c;
      for (UnnamedColumn o : colOp.getOperands()) {
        this.replaceGroupByReference(o);
      }
    } else if (c instanceof BaseColumn) {
      BaseColumn baseCol = (BaseColumn) c;
      String newRef = oldTableAliasMap.get(baseCol.getTableSourceAlias());
      if (newRef != null) baseCol.setTableSourceAlias(newRef);
      else {
        newRef = colNameAndTableAlias.get(baseCol.getColumnName());
        if (newRef != null) baseCol.setTableSourceAlias(newRef);
      }
    }
  }

  private List<OrderbyAttribute> replaceOrderby(
      List<SelectItem> selectItems, List<OrderbyAttribute> orderbyAttributesList)
      throws VerdictDBDbmsException {
    List<OrderbyAttribute> newOrderby = new ArrayList<>();
    for (OrderbyAttribute o : orderbyAttributesList) {
      newOrderby.add(
          new OrderbyAttribute(
              replaceGroupby(selectItems, Arrays.asList(o.getAttribute()), true).get(0),
              o.getOrder()));
    }
    return newOrderby;
  }

  /*
   * return the ColName contained by the table
   */
  private Pair<List<String>, AbstractRelation> setupTableSource(AbstractRelation table)
      throws VerdictDBDbmsException {
    // in order to prevent informal table alias, we replace all table alias
    if (!(table instanceof JoinTable)) {
      if (table.getAliasName().isPresent()) {
        String alias = table.getAliasName().get();
        alias = alias.replace("`", "");
        alias = alias.replace("\"", "");
        oldTableAliasMap.put(alias, verdictTableAliasPrefix + itemID);
      }
      table.setAliasName(verdictTableAliasPrefix + itemID++);
    }
    // if (!table.getAliasName().isPresent() && !(table instanceof JoinTable)) {
    //  table.setAliasName(verdictTableAliasPrefix + itemID++);
    // }
    if (table instanceof BaseTable) {
      BaseTable bt = (BaseTable) table;
      List<String> colName = new ArrayList<>();
      if (bt.getSchemaName() == null) {
        bt.setSchemaName(meta.getDefaultSchema());
      }
      List<Pair<String, String>> cols = meta.getColumns(bt.getSchemaName(), bt.getTableName());
      for (Pair<String, String> c : cols) {
        colNameAndTableAlias.put(c.getKey(), bt.getAliasName().get());
        colName.add(c.getKey());
      }
      tableInfoAndAlias.put(
          ImmutablePair.of(bt.getSchemaName(), bt.getTableName()), table.getAliasName().get());
      return new ImmutablePair<>(colName, table);
      
    } else if (table instanceof JoinTable) {
      List<String> joinColName = new ArrayList<>();
      for (int i = 0; i < ((JoinTable) table).getJoinList().size(); i++) {
        Pair<List<String>, AbstractRelation> result =
            setupTableSource(((JoinTable) table).getJoinList().get(i));
        ((JoinTable) table).getJoinList().set(i, result.getValue());
        joinColName.addAll(result.getKey());
        if (i != 0) {
          ((JoinTable) table)
              .getCondition()
              .set(i - 1, replaceFilter(((JoinTable) table).getCondition().get(i - 1)));
        }
      }
      return new ImmutablePair<>(joinColName, table);
      
    } else if (table instanceof SelectQuery) {
      List<String> colName = new ArrayList<>();
      RelationStandardizer g = new RelationStandardizer(meta, syntax);
      g.oldTableAliasMap.putAll(oldTableAliasMap);
      g.setTableInfoAndAlias(tableInfoAndAlias);
      g.setColNameAndTableAlias(colNameAndTableAlias);
      g.setColNameAndColAlias(colNameAndColAlias);
      String aliasName = table.getAliasName().get();
      table = g.standardize((SelectQuery) table);
      table.setAliasName(aliasName);
      
      // Invariant: Only Aliased Column or Asterisk Column should appear in the subquery
      for (SelectItem sel : ((SelectQuery) table).getSelectList()) {
        if (sel instanceof AliasedColumn) {
          // If the aliased name of the column is replaced by ourselves, we should remember the
          // column name
          if (((AliasedColumn) sel).getColumn() instanceof BaseColumn
              && ((AliasedColumn) sel).getAliasName().matches("^vc[0-9]+$")) {
            colNameAndTableAlias.put(
                ((BaseColumn) ((AliasedColumn) sel).getColumn()).getColumnName(),
                table.getAliasName().get());
            colNameAndTempColAlias.put(
                ((BaseColumn) ((AliasedColumn) sel).getColumn()).getColumnName(),
                ((AliasedColumn) sel).getAliasName());
          } else
            colNameAndTableAlias.put(
                ((AliasedColumn) sel).getAliasName(), table.getAliasName().get());
          colName.add(((AliasedColumn) sel).getAliasName());
          
        } else if (sel instanceof AsteriskColumn) {
          // put all the columns in the fromlist of subquery to the colNameAndTableAlias
          HashMap<String, String> subqueryColumnList = g.getColNameAndTableAlias();
          for (String col : subqueryColumnList.keySet()) {
            colNameAndTableAlias.put(col, table.getAliasName().get());
            colName.add(col);
          }
        }
      }
      return new ImmutablePair<>(colName, table);
    }
    return null;
  }

  /*
   * Figure out the table alias and the columns the table have
   */
  private List<AbstractRelation> setupTableSources(SelectQuery relationToAlias)
      throws VerdictDBDbmsException {
    List<AbstractRelation> fromList = relationToAlias.getFromList();
    for (int i = 0; i < fromList.size(); i++) {
      fromList.set(i, setupTableSource(fromList.get(i)).getValue());
    }
    return fromList;
  }

  public SelectQuery standardize(SelectQuery relationToAlias) throws VerdictDBDbmsException {
    // From (Source)
    List<AbstractRelation> fromList = setupTableSources(relationToAlias);
    
    // Select
    List<SelectItem> selectItemList = replaceSelectList(relationToAlias.getSelectList());
    SelectQuery AliasedRelation = SelectQuery.create(selectItemList, fromList);

    // Filter
    UnnamedColumn where;
    if (relationToAlias.getFilter().isPresent()) {
      where = replaceFilter(relationToAlias.getFilter().get());
      AliasedRelation.addFilterByAnd(where);
    }

    // Group by
    List<GroupingAttribute> groupby;
    if (relationToAlias.getGroupby().size() != 0) {
      groupby = replaceGroupby(selectItemList, relationToAlias.getGroupby());
      AliasedRelation.addGroupby(groupby);
    }

    // Having
    UnnamedColumn having;
    if (relationToAlias.getHaving().isPresent()) {
      having = replaceFilter(relationToAlias.getHaving().get());

      // replace columnOp with alias if possible
      //      if (having instanceof ColumnOp) {
      //        List<ColumnOp> checklist = new ArrayList<>();
      //        checklist.add((ColumnOp) having);
      //        while (!checklist.isEmpty()) {
      //          ColumnOp columnOp = checklist.get(0);
      //          checklist.remove(0);
      //          for (UnnamedColumn operand : columnOp.getOperands()) {
      //            if (operand instanceof ColumnOp) {
      //              if (columnOpAliasMap.containsKey(operand)) {
      //                columnOp.setOperand(
      //                    columnOp.getOperands().indexOf(operand),
      //                    new AliasReference(columnOpAliasMap.get(operand)));
      //              } else checklist.add((ColumnOp) operand);
      //            }
      //            // if (operand instanceof SubqueryColumn) {
      //            //  throw new VerdictDBDbmsException("Do not support subquery in Having
      // clause.");
      //            // }
      //          }
      //        }
      //      }

      // if having clause contains subquery, throw an exception since it is unsupported at
      // the moment
      if (containsSubquery(having)) {
        throw new VerdictDBDbmsException("Currently, subquery is not supported in HAVING clause.");
      }

      AliasedRelation.addHavingByAnd(having);
    }

    // Order by
    List<OrderbyAttribute> orderby;
    if (relationToAlias.getOrderby().size() != 0) {
      orderby = replaceOrderby(selectItemList, relationToAlias.getOrderby());
      AliasedRelation.addOrderby(orderby);
    }

    if (relationToAlias.getLimit().isPresent()) {
      AliasedRelation.addLimit(relationToAlias.getLimit().get());
    }
    return AliasedRelation;
  }

  private boolean containsSubquery(UnnamedColumn having) {
    boolean hasSubquery = false;
    if (having instanceof ColumnOp) {
      ColumnOp op = (ColumnOp) having;
      for (UnnamedColumn operand : op.getOperands()) {
        if (operand instanceof SubqueryColumn) {
          hasSubquery = true;
        } else if (operand instanceof ColumnOp) {
          hasSubquery = hasSubquery | containsSubquery(operand);
        }
      }
    }
    return hasSubquery;
  }

  public HashMap<String, String> getColNameAndColAlias() {
    return colNameAndColAlias;
  }

  public HashMap<Pair<String, String>, String> getTableInfoAndAlias() {
    return tableInfoAndAlias;
  }

  public HashMap<String, String> getColNameAndTableAlias() {
    return colNameAndTableAlias;
  }

  public void setColNameAndTableAlias(HashMap<String, String> colNameAndTableAlias) {
    for (String key : colNameAndTableAlias.keySet()) {
      this.colNameAndTableAlias.put(key, colNameAndTableAlias.get(key));
    }
  }

  public void setTableInfoAndAlias(HashMap<Pair<String, String>, String> tableInfoAndAlias) {
    for (Pair<String, String> key : tableInfoAndAlias.keySet()) {
      this.tableInfoAndAlias.put(key, tableInfoAndAlias.get(key));
    }
  }

  public void setColNameAndColAlias(HashMap<String, String> colNameAndColAlias) {
    for (String key : colNameAndColAlias.keySet()) {
      this.colNameAndColAlias.put(key, colNameAndColAlias.get(key));
    }
  }

  public void setColumnOpAliasMap(HashMap<ColumnOp, String> columnOpAliasMap) {
    this.columnOpAliasMap = columnOpAliasMap;
  }

  public HashMap<ColumnOp, String> getColumnOpAliasMap() {
    return columnOpAliasMap;
  }

  public static void resetItemID() {
    itemID = 1;
  }
}
