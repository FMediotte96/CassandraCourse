package org.example;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class LocalCassandraConnector {

    public static void main(String[] args) {
        Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        Session sess = cluster.connect("firstkeyspace");

        ResultSet rs = sess.execute("select * from firstkeyspace.books_by_author");
        printResult(rs);
    }

    public static void printResult(ResultSet rs) {
        for (Row row : rs) {
            System.out.println(
                row.getString("author_name") + " | "
                    + row.getInt("publish_year") + " | "
                    + row.getFloat("rating") + " | "
            );
        }
    }
}
