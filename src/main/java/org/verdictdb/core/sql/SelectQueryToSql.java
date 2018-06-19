package org.verdictdb.core.sql;

import java.util.List;
import java.util.Set;

import org.verdictdb.core.query.AbstractRelation;
import org.verdictdb.core.query.AliasReference;
import org.verdictdb.core.query.AliasedColumn;
import org.verdictdb.core.query.AsteriskColumn;
import org.verdictdb.core.query.BaseColumn;
import org.verdictdb.core.query.BaseTable;
import org.verdictdb.core.query.ColumnOp;
import org.verdictdb.core.query.ConstantColumn;
import org.verdictdb.core.query.GroupingAttribute;
import org.verdictdb.core.query.JoinTable;
import org.verdictdb.core.query.OrderbyAttribute;
import org.verdictdb.core.query.SelectItem;
import org.verdictdb.core.query.SelectQueryOp;
import org.verdictdb.core.query.SubqueryColumn;
import org.verdictdb.core.query.UnnamedColumn;
import org.verdictdb.exception.UnexpectedTypeException;
import org.verdictdb.exception.ValueException;
import org.verdictdb.exception.VerdictDbException;
import org.verdictdb.sql.syntax.SyntaxAbstract;

import com.google.common.base.Optional;
import com.google.common.collect.Sets;

public class SelectQueryToSql {

  SyntaxAbstract syntax;

  Set<String> opTypeNotRequiringParentheses = Sets.newHashSet(
      "sum", "avg", "count", "std", "sqrt", "notnull", "whenthenelse", "rand", "floor");

  public SelectQueryToSql(SyntaxAbstract syntax) {
    this.syntax = syntax;
  }

  public String toSql(AbstractRelation relation) throws VerdictDbException {
    if (!(relation instanceof SelectQueryOp)) {
      throw new UnexpectedTypeException("Unexpected type: " + relation.getClass().toString());
    }
    return selectQueryToSql((SelectQueryOp) relation);
  }

  String selectItemToSqlPart(SelectItem item) throws VerdictDbException {
    if (item instanceof AliasedColumn) {
      return aliasedColumnToSqlPart((AliasedColumn) item);
    } else if (item instanceof UnnamedColumn) {
      return unnamedColumnToSqlPart((UnnamedColumn) item);
    } else {
      throw new UnexpectedTypeException("Unexpceted argument type: " + item.getClass().toString());
    }
  }

  String aliasedColumnToSqlPart(AliasedColumn acolumn) throws VerdictDbException {
    String aliasName = acolumn.getAliasName();
    return unnamedColumnToSqlPart(acolumn.getColumn()) + " as " + aliasName;
  }

  String groupingAttributeToSqlPart(GroupingAttribute column) throws VerdictDbException {
    if (column instanceof AsteriskColumn) {
      throw new UnexpectedTypeException("asterisk is not expected in the groupby clause.");
    }
    if (column instanceof AliasReference) {
      return quoteName(((AliasReference) column).getAliasName());
    } else {
      return unnamedColumnToSqlPart((UnnamedColumn) column);
    }
  }

  String unnamedColumnToSqlPart(UnnamedColumn column) throws VerdictDbException {
    if (column instanceof BaseColumn) {
      BaseColumn base = (BaseColumn) column;
      return quoteName(base.getTableSourceAlias()) + "." + quoteName(base.getColumnName());
    } else if (column instanceof ConstantColumn) {
      return ((ConstantColumn) column).getValue().toString();
    } else if (column instanceof AsteriskColumn) {
      return "*";
    } else if (column instanceof ColumnOp) {
      ColumnOp columnOp = (ColumnOp) column;
      if (columnOp.getOpType().equals("avg")) {
        return "avg(" + unnamedColumnToSqlPart(columnOp.getOperand()) + ")";
      } else if (columnOp.getOpType().equals("sum")) {
        return "sum(" + unnamedColumnToSqlPart(columnOp.getOperand()) + ")";
      } else if (columnOp.getOpType().equals("count")) {
        return "count(*)";
      } else if (columnOp.getOpType().equals("std")) {
        return "std(" + unnamedColumnToSqlPart(columnOp.getOperand()) + ")";
      } else if (columnOp.getOpType().equals("sqrt")) {
        return "sqrt(" + unnamedColumnToSqlPart(columnOp.getOperand()) + ")";
      } else if (columnOp.getOpType().equals("add")) {
        return withParentheses(columnOp.getOperand(0)) + " + " + withParentheses(columnOp.getOperand(1));
      } else if (columnOp.getOpType().equals("subtract")) {
        return withParentheses(columnOp.getOperand(0)) + " - " + withParentheses(columnOp.getOperand(1));
      } else if (columnOp.getOpType().equals("multiply")) {
        return withParentheses(columnOp.getOperand(0)) + " * " + withParentheses(columnOp.getOperand(1));
      } else if (columnOp.getOpType().equals("divide")) {
        return withParentheses(columnOp.getOperand(0)) + " / " + withParentheses(columnOp.getOperand(1));
      } else if (columnOp.getOpType().equals("pow")) {
        return "pow(" + unnamedColumnToSqlPart(columnOp.getOperand(0)) + ", "
            + unnamedColumnToSqlPart(columnOp.getOperand(1)) + ")";
      } else if (columnOp.getOpType().equals("equal")) {
        return withParentheses(columnOp.getOperand(0)) + " = " + withParentheses(columnOp.getOperand(1));
      } else if (columnOp.getOpType().equals("and")) {
        return withParentheses(columnOp.getOperand(0)) + " and " + withParentheses(columnOp.getOperand(1));
      } else if (columnOp.getOpType().equals("or")) {
        return withParentheses(columnOp.getOperand(0)) + " or " + withParentheses(columnOp.getOperand(1));
      }
//      else if (columnOp.getOpType().equals("casewhenelse")) {
//        return "case " + withParentheses(columnOp.getOperand(0))
//        + " when " + withParentheses(columnOp.getOperand(1))
//        + " else " + withParentheses(columnOp.getOperand(2))
//        + " end";
//      }
      else if (columnOp.getOpType().equals("whenthenelse")) {
        return "case "
            + "when " + withParentheses(columnOp.getOperand(0))
            + " then " + withParentheses(columnOp.getOperand(1))
            + " else " + withParentheses(columnOp.getOperand(2))
            + " end";
      } else if (columnOp.getOpType().equals("notequal")) {
        return withParentheses(columnOp.getOperand(0)) + " <> " + withParentheses(columnOp.getOperand(1));
      } else if (columnOp.getOpType().equals("notnull")) {
        return withParentheses(columnOp.getOperand(0)) + " is not null";
      } else if (columnOp.getOpType().equals("interval")) {
        return "interval " + withParentheses(columnOp.getOperand(0)) + " " + withParentheses(columnOp.getOperand(1));
      } else if (columnOp.getOpType().equals("date")) {
        return "date " + withParentheses(columnOp.getOperand());
      } else if (columnOp.getOpType().equals("greater")) {
        return withParentheses(columnOp.getOperand(0)) + " > " + withParentheses(columnOp.getOperand(1));
      } else if (columnOp.getOpType().equals("less")) {
        return withParentheses(columnOp.getOperand(0)) + " < " + withParentheses(columnOp.getOperand(1));
      } else if (columnOp.getOpType().equals("greaterequal")) {
        return withParentheses(columnOp.getOperand(0)) + " >= " + withParentheses(columnOp.getOperand(1));
      } else if (columnOp.getOpType().equals("lessequal")) {
        return withParentheses(columnOp.getOperand(0)) + " <= " + withParentheses(columnOp.getOperand(1));
      } else if (columnOp.getOpType().equals("min")) {
        return "min(" + selectItemToSqlPart(columnOp.getOperand()) + ")";
      } else if (columnOp.getOpType().equals("max")) {
        return "max(" + selectItemToSqlPart(columnOp.getOperand()) + ")";
      } else if (columnOp.getOpType().equals("min")) {
        return "max(" + selectItemToSqlPart(columnOp.getOperand()) + ")";
      } else if (columnOp.getOpType().equals("floor")) {
        return "floor(" + selectItemToSqlPart(columnOp.getOperand()) + ")";
      } else if (columnOp.getOpType().equals("is")) {
        return withParentheses(columnOp.getOperand(0)) + " is " + withParentheses(columnOp.getOperand(1));
      } else if (columnOp.getOpType().equals("like")) {
        return withParentheses(columnOp.getOperand(0)) + " like " + withParentheses(columnOp.getOperand(1));
      } else if (columnOp.getOpType().equals("notlike")) {
        return withParentheses(columnOp.getOperand(0)) + " not like " + withParentheses(columnOp.getOperand(1));
      } else if (columnOp.getOpType().equals("exists")) {
        return "exists " + withParentheses(columnOp.getOperand());
      } else if (columnOp.getOpType().equals("notexists")) {
        return "not exists " + withParentheses(columnOp.getOperand());
      } else if (columnOp.getOpType().equals("between")) {
        return withParentheses(columnOp.getOperand(0)) 
            + " between " 
            + withParentheses(columnOp.getOperand(1)) 
            + " and " 
            + withParentheses(columnOp.getOperand(2));
      } else if (columnOp.getOpType().equals("in")) {
        List<UnnamedColumn> columns = columnOp.getOperands();
        if (columns.size()==2 && columns.get(1) instanceof SubqueryColumn) {
          return withParentheses(columns.get(0)) + " in " + withParentheses(columns.get(1));
        }
        String temp = "";
        for (int i = 1; i < columns.size(); i++) {
          if (i != columns.size() - 1) {
            temp = temp + withParentheses(columns.get(i)) + ", ";
          } else temp = temp + withParentheses(columns.get(i));
        }
        return withParentheses(columns.get(0)) + " in (" + temp + ")";
      } else if (columnOp.getOpType().equals("notin")) {
        List<UnnamedColumn> columns = columnOp.getOperands();
        if (columns.size()==2 && columns.get(1) instanceof SubqueryColumn) {
          return withParentheses(columns.get(0)) + " not in " + withParentheses(columns.get(1));
        }
        String temp = "";
        for (int i = 1; i < columns.size(); i++) {
          if (i != columns.size() - 1) {
            temp = temp + withParentheses(columns.get(i)) + ", ";
          } else temp = temp + withParentheses(columns.get(i));
        }
        return withParentheses(columns.get(0)) + " not in (" + temp + ")";
      } else if (columnOp.getOpType().equals("countdistinct")) {
        return "count(distinct " + withParentheses(columnOp.getOperand()) + ")";
      } else if (columnOp.getOpType().equals("substr")) {
        return "substr(" + withParentheses(columnOp.getOperand(0)) + ", " +
            withParentheses(columnOp.getOperand(1)) + ", " + withParentheses(columnOp.getOperand(2)) + ")";
      } else if (columnOp.getOpType().equals("rand")) {
        return syntax.randFunction();
      } else {
        throw new UnexpectedTypeException("Unexpceted opType of column: " + columnOp.getOpType().toString());
      }
    } else if (column instanceof SubqueryColumn) {
      return "(" + selectQueryToSql(((SubqueryColumn) column).getSubquery()) + ")";
    }
    throw new UnexpectedTypeException("Unexpceted argument type: " + column.getClass().toString());
  }


  String withParentheses(UnnamedColumn column) throws VerdictDbException {
    String sql = unnamedColumnToSqlPart(column);
    if (column instanceof ColumnOp && !opTypeNotRequiringParentheses.contains(((ColumnOp) column).getOpType())) {
      sql = "(" + sql + ")";
    }
    return sql;
  }

  String selectQueryToSql(SelectQueryOp sel) throws VerdictDbException {
    StringBuilder sql = new StringBuilder();

    // select
    sql.append("select");
    List<SelectItem> columns = sel.getSelectList();
    boolean isFirstColumn = true;
    for (SelectItem a : columns) {
      if (isFirstColumn) {
        sql.append(" " + selectItemToSqlPart(a));
        isFirstColumn = false;
      } else {
        sql.append(", " + selectItemToSqlPart(a));
      }
    }

    // from
    sql.append(" from");
    List<AbstractRelation> rels = sel.getFromList();
    boolean isFirstRel = true;
    for (AbstractRelation r : rels) {
      if (isFirstRel) {
        sql.append(" " + relationToSqlPart(r));
        isFirstRel = false;
      } else {
        sql.append(", " + relationToSqlPart(r));
      }
    }

    // where
    Optional<UnnamedColumn> filter = sel.getFilter();
    if (filter.isPresent()) {
      sql.append(" where ");
      sql.append(unnamedColumnToSqlPart(filter.get()));
    }

    // groupby
    List<GroupingAttribute> groupby = sel.getGroupby();
    boolean isFirstGroup = true;
    for (GroupingAttribute a : groupby) {
      if (isFirstGroup) {
        sql.append(" group by ");
        sql.append(groupingAttributeToSqlPart(a));
        isFirstGroup = false;
      } else {
        sql.append(", " + groupingAttributeToSqlPart(a));
      }
    }

    //having
    Optional<UnnamedColumn> having = sel.getHaving();
    if (having.isPresent()) {
      sql.append(" having ");
      sql.append(unnamedColumnToSqlPart(having.get()));
    }

    //orderby
    List<OrderbyAttribute> orderby = sel.getOrderby();
    boolean isFirstOrderby = true;
    for (OrderbyAttribute a : orderby) {
      if (isFirstOrderby) {
        sql.append(" order by ");
        sql.append(quoteName(a.getAttributeName()));
        sql.append(" " + a.getOrder());
        isFirstOrderby = false;
      } else {
        sql.append(", " + quoteName(a.getAttributeName()));
        sql.append(" " + a.getOrder());
      }
    }

    //Limit
    Optional<UnnamedColumn> limit = sel.getLimit();
    if (limit.isPresent()) {
      sql.append(" limit " + unnamedColumnToSqlPart(limit.get()));
    }

    return sql.toString();
  }

  String relationToSqlPart(AbstractRelation relation) throws VerdictDbException {
    StringBuilder sql = new StringBuilder();

    if (relation instanceof BaseTable) {
      BaseTable base = (BaseTable) relation;
      sql.append(quoteName(base.getSchemaName()) + "." + quoteName(base.getTableName()));
      if (base.getAliasName().isPresent()) {
        sql.append(" as " + base.getAliasName().get());
      }
      return sql.toString();
    }

    if (relation instanceof JoinTable) {
      //sql.append("(");
      sql.append(relationToSqlPart(((JoinTable) relation).getJoinList().get(0)));
      for (int i = 1; i < ((JoinTable) relation).getJoinList().size(); i++) {
        if (((JoinTable) relation).getJoinTypeList().get(i - 1).equals(JoinTable.JoinType.inner)) {
          sql.append(" inner join ");
        } else if (((JoinTable) relation).getJoinTypeList().get(i - 1).equals(JoinTable.JoinType.outer)) {
          sql.append(" outer join ");
        } else if (((JoinTable) relation).getJoinTypeList().get(i - 1).equals(JoinTable.JoinType.left)) {
          sql.append(" left join ");
        } else if (((JoinTable) relation).getJoinTypeList().get(i - 1).equals(JoinTable.JoinType.leftouter)) {
          sql.append(" left outer join ");
        } else if (((JoinTable) relation).getJoinTypeList().get(i - 1).equals(JoinTable.JoinType.right)) {
          sql.append(" right join ");
        } else if (((JoinTable) relation).getJoinTypeList().get(i - 1).equals(JoinTable.JoinType.rightouter)) {
          sql.append(" right outer join ");
        }
        sql.append(relationToSqlPart(((JoinTable) relation).getJoinList().get(i)) + " on " +
            withParentheses(((JoinTable) relation).getCondition().get(i - 1)));
      }
      //sql.append(")");
      if (((JoinTable) relation).getAliasName().isPresent()) {
        sql.append(" as " + ((JoinTable) relation).getAliasName().toString());
      }
      return sql.toString();
    }

    if (!(relation instanceof SelectQueryOp)) {
      throw new UnexpectedTypeException("Unexpected relation type: " + relation.getClass().toString());
    }

    SelectQueryOp sel = (SelectQueryOp) relation;
    Optional<String> aliasName = sel.getAliasName();
    if (!aliasName.isPresent()) {
      throw new ValueException("An inner select query must be aliased.");
    }

    return "(" + selectQueryToSql(sel) + ") as " + aliasName.get();
  }

  String quoteName(String name) {
    String quoteString = syntax.getQuoteString();
    return quoteString + name + quoteString;
  }
}