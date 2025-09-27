package com.ac.iisc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class Calcite
{
    public static void main(String[] args) 
    {
        String pgHost = "localhost", pgPort="5432", pgDB="tpch", pgUser="postgres", pgPwd="123";
        

        String jdbcUrl = String.format("jdbc:postgresql://%s:%s/%s", pgHost, pgPort, pgDB);

        String model = "{\n" +
                "  \"version\": \"1.0\",\n" +
                "  \"defaultSchema\": \"pg\",\n" +
                "  \"schemas\": [\n" +
                "    {\n" +
                "      \"name\": \"pg\",\n" +
                "      \"type\": \"jdbc\",\n" +
                "      \"jdbcDriver\": \"org.postgresql.Driver\",\n" +
                "      \"jdbcUrl\": \"" + jdbcUrl + "\",\n" +
                "      \"jdbcUser\": \"" + pgUser + "\",\n" +
                "      \"jdbcPassword\": \"" + pgPwd + "\"\n" +
                "    }\n" +
                "  ]\n" +
                "}";
        
        //Create Calcite connection
        Properties info = new Properties();
        Connection calciteConn;
        Statement stmt;
        ResultSet rs = null;

        info.setProperty("model", "inline:" + model);
        info.setProperty("unquotedCasing", "TO_LOWER");

        try
        {
            calciteConn = DriverManager.getConnection("jdbc:calcite:", info);

            //Execute a query using calcite
            stmt = calciteConn.createStatement();

            System.out.println("Executing query: SELECT * FROM pg.nation LIMIT 5");
            rs = stmt.executeQuery("SELECT * FROM pg.nation LIMIT 5");

            while (rs.next()) {
                // Assuming the 'nation' table has these columns from the TPC-H schema
                int nationKey = rs.getInt("n_nationkey");
                String name = rs.getString("n_name");
                int regionKey = rs.getInt("n_regionkey");
                System.out.printf("NationKey: %d, Name: %s, RegionKey: %d%n", nationKey, name, regionKey);
            }

            rs.close();
            stmt.close();
            calciteConn.close();
        }
        catch (SQLException ex){ex.printStackTrace();}
    }
}
