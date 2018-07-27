package org.apache.drill;

import ch.qos.logback.classic.Level;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.compile.ClassBuilder;
import org.apache.drill.test.ClientFixture;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.DrillTest;
import org.apache.drill.test.BaseDirTestWatcher;
import org.apache.drill.test.LogFixture;
import org.apache.drill.test.TestTools;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;


public class MD4692 extends DrillTest {

  public static final String GENERATED_SOURCES_DIR = "/Users/karthik/drill/generated-code/";
  @Rule
  public final BaseDirTestWatcher dirTestWatcher = new BaseDirTestWatcher();
  @Rule public final TestRule TIMEOUT = new DisableOnDebug(TestTools.getTimeoutRule(10_000_000));

  @Test
  public void testMD4692() throws Exception {

    LogFixture.LogFixtureBuilder logBuilder = LogFixture.builder()
            .logger("org.apache.drill", Level.TRACE).toConsole();

    try (//LogFixture logs = logBuilder.build();
         ClusterFixture cluster = ClusterFixture.builder(dirTestWatcher)
                 .configProperty(ClassBuilder.CODE_DIR_OPTION, GENERATED_SOURCES_DIR)
                 .configProperty(ExecConstants.BIT_RPC_TIMEOUT, 0)
                 .configProperty(ExecConstants.USER_RPC_TIMEOUT, 0)
                 .configProperty(ExecConstants.BIT_TIMEOUT, 0)
                 .build();


         ClientFixture client = cluster.clientFixture()) {
      cluster.defineWorkspace("dfs", "data", "/Users/karthik/work/bugs/", "csv");
      String sql = "select * from ( " +
              "select " +
              "ntile(10) over (order by Index) IntegerValuea, " +
              "ntile(10) over (order by Index) IntegerValueb, " +
              "ntile(10) over (order by Index) IntegerValuec, " +
              "ntile(10) over (order by Index) IntegerValued, " +
              "ntile(10) over (order by Index) IntegerValuee " +
              "from (select * from dfs.data.`MD4692/character5_nulls_1MB_1GB.parquet` " +
              "order by CharacterValuea) d where d.CharacterValuea = '1234567890123100')";
      client.queryBuilder().sql(sql).printCsv();
    }
  }
}