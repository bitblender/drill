package org.apache.drill;

import ch.qos.logback.classic.Level;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.compile.ClassBuilder;
import org.apache.drill.test.BaseDirTestWatcher;
import org.apache.drill.test.ClientFixture;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.LogFixture;
import org.apache.drill.test.TestTools;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;

public class MD4798 {
  public static final String GENERATED_SOURCES_DIR = "/Users/karthik/drill/generated-code/";
  @Rule
  public final BaseDirTestWatcher dirTestWatcher = new BaseDirTestWatcher();
  @Rule
  public final TestRule TIMEOUT = new DisableOnDebug(TestTools.getTimeoutRule(10_000_000));

  @Test
  public void testMD4798() throws Exception {
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
      //String sql = "select BooleanValue as foo from dfs.data.`MD4608/alltypes_small_1MB_1GB.parquet`";
      //String sql = "select * from (select age(cast(to_date(cast(cume_dist(length(substr(concat(BinaryValue, VarcharValue),10,8000))) over (order by Index) as Integer)) as Timestamp), '2018-05-10') IntervalSecondValuea, 'Fq7gIk2x1FPI8nQUXG7SqBcXKx7ocIBa17CNC9KWtrlW7gZykOfHVQhipHs5C5DDAZWhb0EwxnEX16TA7Eb6HTC1 wMr3w0sPhVG' NewCharacterValue, Index, BigIntValue, BooleanValue, DateValue, FloatValue, DoubleValue, NullValue, IntegerValue, TimeValue, TimestampValue, IntervalYearValue, IntervalDayValue, IntervalSecondValue, age(cast(to_date(cast(cume_dist(length(substr(concat(BinaryValue, VarcharValue),10,8000))) over (order by Index) as Integer)) as Timestamp), '2018-05-10') IntervalSecondValueb from (select * from dfs.data.`/MD4608/alltypes_small_1MB_1GB.parquet` order by BigIntvalue)) d where d.Index = 1";
      String sql = "select * from (" +
              " select split(CharacterValuea, '0') CharacterValuea,  split(CharacterValueb, '1') CharacterValueb, split(CharacterValuec, '2') CharacterValuec, " +
              " split(CharacterValued, '3') CharacterValued," +
              " split(CharacterValuee, '9') CharacterValuee" +
              " from (select * from dfs.data.`MD4798/character5_1MB_1GB.parquet` order by CharacterValuea) d where d.CharacterValuea = '1234567890123110')";
      client.queryBuilder().sql(sql).printCsv();
    }
  }
}

