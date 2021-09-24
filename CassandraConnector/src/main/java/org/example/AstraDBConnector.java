package org.example;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

import java.nio.file.Paths;

public class AstraDBConnector {

    public static void main(String[] args) {
        CqlSession session = CqlSession.builder()
            .withCloudSecureConnectBundle(Paths.get("/home/facundo.mediotte/Facu/Udemy/Cassandra/secure-connect-sampledb.zip"))
            .withAuthCredentials("HRyCERDbTGrxYrPDdsYOtmDY", "4ZquvG9vOPXg+ugoGeqgQemxmOFgoq0PuN9Hfo7bvvC3S5jBFBDxaZCZkey+UZ0woSddfLRZd4P-S7y6QyPOkXYubWkGSSu5G7tg2X0vXTXezbkRJZff5mZ_nMSFfKZ5")
            .build();

        ResultSet rs = session.execute("select * from firstkeyspace.books_by_author");
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
