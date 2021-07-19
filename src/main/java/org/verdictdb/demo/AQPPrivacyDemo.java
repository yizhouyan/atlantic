package org.verdictdb.demo;

import java.sql.*;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class AQPPrivacyDemo {
    public static void loadDataset() throws SQLException {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        // Suppose username is root and password is rootpassword.
//        Connection conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "yizhouyan");
        Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/yizhouyan",
                "yizhouyan", "none");
        Statement stmt = conn.createStatement();
//        stmt.execute("CREATE SCHEMA myschema");
        stmt.execute("CREATE TABLE myschema.sales (" +
                "  product   varchar(100)," +
                "  price     real)");

        // insert 10000 rows
        List<String> productList = Arrays.asList("milk", "egg", "juice");
        for (int i = 0; i < 10000; i++) {
            int randInt = ThreadLocalRandom.current().nextInt(0, 3);
            String product = productList.get(randInt);
            double price = (randInt + 2) * 10 + ThreadLocalRandom.current().nextInt(0, 10);
            stmt.execute(String.format(
                    "INSERT INTO myschema.sales (product, price) VALUES('%s', %.0f)",
                    product, price));
        }

        stmt.execute("CREATE TABLE myschema.sales2 (" +
                "  product   varchar(100)," +
                "  price     real)");

        // insert 10000 rows
        for (int i = 0; i < 10000; i++) {
            int randInt = ThreadLocalRandom.current().nextInt(0, 3);
            String product = productList.get(randInt);
            double price = (randInt + 2) * 10 + ThreadLocalRandom.current().nextInt(0, 10);
            stmt.execute(String.format(
                    "INSERT INTO myschema.sales2 (product, price) VALUES('%s', %.0f)",
                    product, price));
        }
    }

    public static void connectToVerdict() throws SQLException {
        Connection verdict =
                DriverManager.getConnection("jdbc:verdict:postgresql://localhost:5432/yizhouyan", "yizhouyan", "none");
//                DriverManager.getConnection("jdbc:verdict:mysql://localhost:3306/test", "root", "yizhouyan");
        Statement vstmt = verdict.createStatement();
        // Use CREATE SCRAMBLE syntax to create scrambled tables.
//        vstmt.execute("CREATE SCRAMBLE myschema.sales_scrambled from myschema.sales BLOCKSIZE 100");
//        vstmt.execute("CREATE SCRAMBLE myschema.sales_scrambled2 from myschema.sales2 BLOCKSIZE 100");
//        ResultSet rs = vstmt.executeQuery(
//                "SELECT count(*) as cnt_products "+
//                        "FROM myschema.sales_scrambled s join myschema.sales2 t " +
//                        "on s.price = t.price ");
//        ResultSet rs = vstmt.executeQuery(
//                "SELECT product, count(price) as cnt_products " +
//                        "FROM myschema.sales_scrambled s group by product");
        ResultSet rs = vstmt.executeQuery(
                "SELECT product, count(price) as cnt_products "+
                        "FROM myschema.sales_scrambled s group by product");
        System.out.println("Producing final results....");
        while (rs.next()) {
            System.out.println(rs.getInt("cnt_products"));
        }
    }

    public static void main(String[] args) throws SQLException {
//        loadDataset();
        connectToVerdict();
    }
}
