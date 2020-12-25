package org.verdictdb.core.scrambling;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.connection.JdbcConnection;
import org.verdictdb.core.execplan.ExecutionInfoToken;
import org.verdictdb.core.querying.ExecutableNodeBase;
import org.verdictdb.core.sqlobject.BaseColumn;
import org.verdictdb.core.sqlobject.ColumnOp;
import org.verdictdb.core.sqlobject.ConstantColumn;
import org.verdictdb.core.sqlobject.SqlConvertible;
import org.verdictdb.core.sqlobject.UnnamedColumn;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.sqlsyntax.MysqlSyntax;
import org.verdictdb.sqlwriter.QueryToSql;

public class UniformScramblingNodeTest {

  private static Connection mysqlConn;

  private static Statement mysqlStmt;

  private static final String MYSQL_HOST;

  private static final String MYSQL_DATABASE = "test";

  private static final String MYSQL_UESR;

  private static final String MYSQL_PASSWORD = "";

  static {
    String env = System.getenv("BUILD_ENV");
    if (env != null && (env.equals("GitLab") || env.equals("DockerCompose"))) {
      MYSQL_HOST = "mysql";
      MYSQL_UESR = "root";
    } else {
      MYSQL_HOST = "localhost";
      MYSQL_UESR = "root";
    }
  }

  @BeforeClass
  public static void setupMySqlDatabase() throws SQLException {
    String mysqlConnectionString =
        String.format("jdbc:mysql://%s/%s?autoReconnect=true&useSSL=false", MYSQL_HOST, MYSQL_DATABASE);
    mysqlConn = DriverManager.getConnection(mysqlConnectionString, MYSQL_UESR, MYSQL_PASSWORD);

    mysqlConn.createStatement().execute("create schema if not exists oldschema");
    mysqlConn.createStatement().execute("create schema if not exists newschema");
    mysqlConn.createStatement().execute("drop table if exists oldschema.oldtable");
    mysqlConn.createStatement().execute("create table if not exists oldschema.oldtable (id smallint)");
    for (int i = 0; i < 6; i++) {
      mysqlConn.createStatement().execute(String.format("insert into oldschema.oldtable values (%d)", i));
    }
  }

  @AfterClass
  public static void tearDown() throws SQLException {
    mysqlConn.createStatement().execute("drop table if exists oldschema.oldtable");
    mysqlConn.createStatement().execute("drop schema if exists oldschema");
    mysqlConn.createStatement().execute("drop table if exists newschema.newtable");
    mysqlConn.createStatement().execute("drop schema if exists newschema");
  }
  
  @Test
  public void testUniformMethodStatisticsWithPredicate() throws VerdictDBException {
    String oldSchemaName = "oldschema";
    String oldTableName = "oldtable";
    int blockSize = 2;    // 2 blocks will be created (since the total is 4 due to a predicate)
    ScramblingMethod method = new UniformScramblingMethod(blockSize);
    Map<String, String> options = new HashMap<>();
    options.put("tierColumnName", "tiercolumn");
    options.put("blockColumnName", "blockcolumn");
    UnnamedColumn predicate = ColumnOp.greaterequal(
        new BaseColumn("t", "id"),  
        ConstantColumn.valueOf(2));
    
    List<ExecutableNodeBase> statNodes = 
        method.getStatisticsNode(oldSchemaName, oldTableName, predicate, null, null, null);
    ExecutableNodeBase statNode = statNodes.get(0);
    
    // set empty tokens
    List<ExecutionInfoToken> tokens = new ArrayList<>();
    tokens.add(new ExecutionInfoToken());
    
    SqlConvertible query = statNode.createQuery(tokens);
    String sql = QueryToSql.convert(new MysqlSyntax(), query);
    assertEquals(
        "select count(*) as `verdictdbtotalcount` " +
        "from `oldschema`.`oldtable` as t " +
        "where t.`id` >= 2",
        sql);
  }
  
  // We will create a scramble table only for the rows that satisfy >= 2 (i.e., 4 rows)
  @Test
  public void testScramblingNodeCreationWithPredicate() throws VerdictDBException, SQLException {
    String newSchemaName = "newschema";
    String newTableName = "newtable";
    String oldSchemaName = "oldschema";
    String oldTableName = "oldtable";
    int blockSize = 2;    // 2 blocks will be created (since the total is 4 due to a predicate)
    ScramblingMethod method = new UniformScramblingMethod(blockSize);
    Map<String, String> options = new HashMap<>();
    options.put("tierColumnName", "tiercolumn");
    options.put("blockColumnName", "blockcolumn");
//    options.put("blockCount", "3");

    // query result
    String sql = "select count(*) as `verdictdbtotalcount` from `oldschema`.`oldtable` as t"
        + " where id >= 2";
    DbmsConnection conn = JdbcConnection.create(mysqlConn);
    DbmsQueryResult queryResult = conn.execute(sql);
    
    UnnamedColumn predicate = ColumnOp.greaterequal(
        new BaseColumn("t", "id"),  
        ConstantColumn.valueOf(2));

    ScramblingNode node = ScramblingNode.create(
        newSchemaName, newTableName,
        oldSchemaName, oldTableName,
        method, predicate, options, new PrivacyMeta());

    // set tokens
    List<ExecutionInfoToken> tokens = new ArrayList<>();
    ExecutionInfoToken e = new ExecutionInfoToken();
    e.setKeyValue(TableSizeCountNode.class.getSimpleName(), queryResult);
    tokens.add(e);

    e = new ExecutionInfoToken();
    e.setKeyValue("schemaName", newSchemaName);
    e.setKeyValue("tableName", newTableName);
    tokens.add(e);

    e = new ExecutionInfoToken();
    List<Pair<String, String>> columnNamesAndTypes = new ArrayList<>();
    columnNamesAndTypes.add(Pair.of("id", "smallint"));
    e.setKeyValue(ScramblingPlan.COLUMN_METADATA_KEY, columnNamesAndTypes);
    tokens.add(e);

    SqlConvertible query = node.createQuery(tokens);
    sql = QueryToSql.convert(new MysqlSyntax(), query);
    String expected = "create table `newschema`.`newtable` "
        + "partition by list columns (`blockcolumn`) ("
        + "partition p0 values in (0), "
        + "partition p1 values in (1)) "
        + "select t.`id`, 0 as `tiercolumn`, "
        + "cast(floor(rand() * 2) as unsigned) as `blockcolumn` "
        + "from `oldschema`.`oldtable` as t "
        + "where t.`id` >= 2";
    assertEquals(expected, sql);
    mysqlConn.createStatement().execute("drop table if exists newschema.newtable");
    mysqlConn.createStatement().execute(sql);
  }

  @Test
  public void testScramblingNodeCreation() throws VerdictDBException, SQLException {
    String newSchemaName = "newschema";
    String newTableName = "newtable";
    String oldSchemaName = "oldschema";
    String oldTableName = "oldtable";
    int blockSize = 2;    // 3 blocks will be created
    ScramblingMethod method = new UniformScramblingMethod(blockSize);
    Map<String, String> options = new HashMap<>();
    options.put("tierColumnName", "tiercolumn");
    options.put("blockColumnName", "blockcolumn");
//    options.put("blockCount", "3");

    // query result
    String sql = "select count(*) as `verdictdbtotalcount` from `oldschema`.`oldtable` as t";
    DbmsConnection conn = JdbcConnection.create(mysqlConn);
    DbmsQueryResult queryResult = conn.execute(sql);

    ScramblingNode node = ScramblingNode.create(
        newSchemaName, newTableName,
        oldSchemaName, oldTableName,
        method, options, new PrivacyMeta());

    // set tokens
    List<ExecutionInfoToken> tokens = new ArrayList<>();
    ExecutionInfoToken e = new ExecutionInfoToken();
    e.setKeyValue(TableSizeCountNode.class.getSimpleName(), queryResult);
    tokens.add(e);

    e = new ExecutionInfoToken();
    e.setKeyValue("schemaName", newSchemaName);
    e.setKeyValue("tableName", newTableName);
    tokens.add(e);

    e = new ExecutionInfoToken();
    List<Pair<String, String>> columnNamesAndTypes = new ArrayList<>();
    columnNamesAndTypes.add(Pair.of("id", "smallint"));
    e.setKeyValue(ScramblingPlan.COLUMN_METADATA_KEY, columnNamesAndTypes);
    tokens.add(e);

    SqlConvertible query = node.createQuery(tokens);
    sql = QueryToSql.convert(new MysqlSyntax(), query);
    String expected = "create table `newschema`.`newtable` "
        + "partition by list columns (`blockcolumn`) ("
        + "partition p0 values in (0), "
        + "partition p1 values in (1), "
        + "partition p2 values in (2)) "
        + "select t.`id`, 0 as `tiercolumn`, "
        + "cast(floor(rand() * 3) as unsigned) as `blockcolumn` "
        + "from `oldschema`.`oldtable` as t";
    assertEquals(expected, sql);
    mysqlConn.createStatement().execute("drop table if exists newschema.newtable");
    mysqlConn.createStatement().execute(sql);
  }

}
