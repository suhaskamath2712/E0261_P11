

import javax.sql.DataSource;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.Lex;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.schema.SchemaPlus;
import static org.apache.calcite.sql.SqlDialect.DatabaseProduct.POSTGRESQL;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;

public class QueryPlanEquivalenceTest
{
    private static Planner planner;

    private static DataSource createDataSource(String dbUrl, String dbUser, String dbPassword)
    {
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setURL(dbUrl);
        dataSource.setUser(dbUser);
        dataSource.setPassword(dbPassword);
        return dataSource;
    }

    @BeforeAll
    public static void setup()
    {
        //Postgresql info
        String dbUrl = "jdbc:postgresql://localhost:5432/test";
        String dbUser = "postgres";
        String dbPassword = "123";
        String dbSchema = "public";

        //Connect to Postgresql
        DataSource dataSource = createDataSource(dbUrl, dbUser, dbPassword);
        SchemaPlus rootSchema = Frameworks.createRootSchema(true);
        rootSchema.add("test", new JdbcSchema(dataSource, POSTGRESQL.getDialect(), null, null, dbSchema));

        //Because Apache Calcite does not have a built-in dialect for Postgresql, we configure it manually
        FrameworkConfig config = Frameworks.newConfigBuilder()
                                .parserConfig(SqlParser.config()
                                    .withLex(Lex.ORACLE) // Start with a base that supports similar features
                                    .withIdentifierMaxLength(128) // Set Postgres-compatible identifier length
                                    .withQuoting(Quoting.DOUBLE_QUOTE) // Use double quotes for identifiers
                                    .withUnquotedCasing(Casing.TO_LOWER) // Unquoted identifiers are lower-cased
                                    .withCaseSensitive(false)) // Case-insensitivity for identifiers
                                .defaultSchema(rootSchema)
                                .build();
        planner = Frameworks.getPlanner(config);
    }

    private RelNode getLogicalPlan(String sql) throws Exception {
        SqlNode sqlNode = planner.parse(sql);
        SqlNode validatedSqlNode = planner.validate(sqlNode);
        RelRoot relRoot = planner.rel(validatedSqlNode);
        return relRoot.rel;
    }

    @Test
    void testEquivalentPlans() throws Exception
    {
    String sql1 = "SELECT * FROM test.cars WHERE brand = 'Toyota' AND test.cars.\"year\" < 2000";
    String sql2 = "SELECT * FROM test.cars WHERE test.cars.\"year\" < 2000 AND brand = 'Toyota'";

        RelNode plan1 = getLogicalPlan(sql1);
        RelNode plan2 = getLogicalPlan(sql2);

        System.out.println("Plan 1:\n" + RelOptUtil.toString(plan1));
        System.out.println("Plan 2:\n" + RelOptUtil.toString(plan2));

        boolean areEquivalent = RelOptUtil.areRowTypesEqual(plan1.getRowType(), plan2.getRowType(), false);
        System.out.println("Are the two plans equivalent? " + areEquivalent);
    }

    @Test
    void testNonEquivalentPlans() throws Exception
    {
        String sql1 = "SELECT * FROM test.cars WHERE brand = 'Toyota' AND test.cars.\"year\" < 2000";
        String sql2 = "SELECT * FROM test.cars WHERE test.cars.\"year\" < 2000";

        RelNode plan1 = getLogicalPlan(sql1);
        RelNode plan2 = getLogicalPlan(sql2);

        System.out.println("Plan 1:\n" + RelOptUtil.toString(plan1));
        System.out.println("Plan 2:\n" + RelOptUtil.toString(plan2));

        boolean areEquivalent = RelOptUtil.areRowTypesEqual(plan1.getRowType(), plan2.getRowType(), false);
        System.out.println("Are the two plans equivalent? " + areEquivalent);
    }
}