#Step-by-step Java Tutorial
We will install VerdictDB, create a connection, and issue a simple query to VerdictDB. In this Quickstart Guide, we will use MySQL for VerdictDB's backend database. See [Connecting to Databases](/reference/connection/) for the examples of connecting to other databases.

##Installation
Create an [empty Maven project](https://maven.apache.org/guides/getting-started/maven-in-five-minutes.html) and
place the verdictdb dependency in the `<dependencies>` of your pom.xml.

```
<dependency>
    <groupId>org.atlantic</groupId>
    <artifactId>verdictdb-core</artifactId>
    <version>{{verdictdb.version}}</version>
</dependency>

<!-- To use MySQL, add the following entry as well: -->
<dependency>
    <groupId>mysql</groupId>
    <artifactId>mysql-connector-java</artifactId>
    <version>5.1.46</version>
</dependency>
```

##Create Table and Insert Data
We will first generate small data to play with. Suppose MySQL is set up as described [here](/tutorial/setup/mysql/).
```
Connection mysqlConn =
    DriverManager.getConnection("jdbc:mysql://localhost", "root", "");
Statement stmt = mysqlConn.createStatement();
stmt.execute("CREATE SCHEMA myschema");
stmt.execute("CREATE TABLE myschema.sales (" +
             "  product   varchar(100)," +
             "  price     double)");

// insert 1000 rows
List<String> productList = Arrays.asList("milk", "egg", "juice");
for (int i = 0; i < 1000; i++) {
  int randInt = ThreadLocalRandom.current().nextInt(0, 3);
  String product = productList.get(randInt);
  double price = (randInt+2) * 10 + ThreadLocalRandom.current().nextInt(0, 10);
  stmt.execute(String.format(
      "INSERT INTO myschema.sales (product, price) VALUES('%s', %.0f)",
      product, price));
}
```

##Create Sample
Create a JDBC connection to VerdictDB.
```
Connection verdict =
    DriverManager.getConnection("jdbc:verdict:mysql://localhost", "root", "");
Statement vstmt = verdict.createStatement();
```

Create a special table called a "scramble", which is the replica of `sales` with extra information VerdictDB uses for speeding up query processing.
```
vstmt.execute("CREATE SCRAMBLE myschema.sales_scrambled from myschema.sales");
```

##Run Queries
Run a regular query to the original table.
```
ResultSet rs = vstmt.executeQuery(
    "SELECT product, AVG(price) "+
    "FROM myschema.sales " +
    "GROUP BY product " +
    "ORDER BY product");
```

## Complete Example Script


```
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class FirstVerdictDBExample {


  public static void main(String args[]) throws SQLException {
    // Suppose username is root and password is rootpassword.
    Connection mysqlConn =
        DriverManager.getConnection("jdbc:mysql://localhost", "root", "");
    Statement stmt = mysqlConn.createStatement();
    stmt.execute("CREATE SCHEMA myschema");
    stmt.execute("CREATE TABLE myschema.sales (" +
                 "  product   varchar(100)," +
                 "  price     double)");

    // insert 1000 rows
    List<String> productList = Arrays.asList("milk", "egg", "juice");
    for (int i = 0; i < 1000; i++) {
      int randInt = ThreadLocalRandom.current().nextInt(0, 3)
      String product = productList.get(randInt);
      double price = (randInt+2) * 10 + ThreadLocalRandom.current().nextInt(0, 10);
      stmt.execute(String.format(
          "INSERT INTO myschema.sales (product, price) VALUES('%s', %.0f)",
          product, price));
    }

    Connection verdict =
        DriverManager.getConnection("jdbc:verdict:mysql://localhost", "root", "");
    Statement vstmt = verdict.createStatement();

    // Use CREATE SCRAMBLE syntax to create scrambled tables.
    vstmt.execute("CREATE SCRAMBLE myschema.sales_scrambled from myschema.sales");

    ResultSet rs = vstmt.executeQuery(
        "SELECT product, AVG(price) "+
        "FROM myschema.sales " +
        "GROUP BY product " +
        "ORDER BY product");
    ```

    // Do something after getting the results.
  }
}
```
