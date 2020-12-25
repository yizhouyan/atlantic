package org.verdictdb.demo;

import java.sql.*;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.verdictdb.commons.VerdictDBLogger;

public class AQPPrivacyDemo {
    public static void loadDataset() throws SQLException{
        // Suppose username is root and password is rootpassword.
        Connection mysqlConn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "yizhouyan");
        Statement stmt = mysqlConn.createStatement();
        stmt.execute("CREATE SCHEMA myschema");
        stmt.execute("CREATE TABLE myschema.sales (" +
                "  product   varchar(100)," +
                "  price     double)");

        // insert 1000 rows
        List<String> productList = Arrays.asList("milk", "egg", "juice");
        for (int i = 0; i < 10000; i++) {
            int randInt = ThreadLocalRandom.current().nextInt(0, 3);
            String product = productList.get(randInt);
            double price = (randInt+2) * 10 + ThreadLocalRandom.current().nextInt(0, 10);
            stmt.execute(String.format(
                    "INSERT INTO myschema.sales (product, price) VALUES('%s', %.0f)",
                    product, price));
        }
    }
    public static void connectToVerdict() throws SQLException{
        Connection verdict =
                DriverManager.getConnection("jdbc:verdict:mysql://localhost:3306/test", "root", "yizhouyan");
        Statement vstmt = verdict.createStatement();
        // Use CREATE SCRAMBLE syntax to create scrambled tables.
//        vstmt.execute("CREATE SCRAMBLE myschema.sales_scrambled from myschema.sales BLOCKSIZE 100");

        ResultSet rs = vstmt.executeQuery(
                "SELECT product, count(price) as cnt_products "+
                        "FROM myschema.sales_scrambled group by product");
        System.out.println("Producing final results....");
        while(rs.next()){
            System.out.println(rs.getString("product"));
            System.out.println(rs.getInt("cnt_products"));
        }
    }
    public static void main(String [] args) throws SQLException{
//        loadDataset();
        VerdictDBLogger.setConsoleLogLevel("debug");
        connectToVerdict();
    }
}
