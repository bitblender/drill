package org.apache.drill.bugs;

import ch.qos.logback.classic.Level;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.compile.ClassBuilder;
import org.apache.drill.test.BaseDirTestWatcher;
import org.apache.drill.test.ClientFixture;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.LogFixture;
import org.apache.drill.test.DrillTest;
import org.junit.Rule;
import org.junit.Test;

public class MD4517 extends DrillTest {

  public static final String GENERATED_SOURCES_DIR = "/Users/karthik/drill/generated-code/";
  @Rule
  public final BaseDirTestWatcher dirTestWatcher = new BaseDirTestWatcher();

  @Test
  public void testMD4517() throws Exception {

    LogFixture.LogFixtureBuilder logBuilder = LogFixture.builder()
            .logger("org.apache.drill.exec.physical.impl.project", Level.TRACE).toConsole();

    try (LogFixture logs = logBuilder.build();
         ClusterFixture cluster = ClusterFixture.builder(dirTestWatcher)
                 .configProperty(ClassBuilder.CODE_DIR_OPTION, GENERATED_SOURCES_DIR)
                 .configProperty(ExecConstants.BIT_RPC_TIMEOUT, 0)
                 .configProperty(ExecConstants.USER_RPC_TIMEOUT, 0)
                 .configProperty(ExecConstants.BIT_TIMEOUT, 0)
                 .build();


         ClientFixture client = cluster.clientFixture()) {
         cluster.defineWorkspace("dfs", "data", "/Users/karthik/work/bugs/MD-4517", "csv");
//            String sql = "select CAST(case isnumeric(columns[0]) WHEN 0 THEN 2147483647 ELSE columns[0] END AS BIGINT) from `dfs.data`.`pw2.csv` ";
//            String sql = "select * from cp.`store/json/input2.json`";
//            String sql = "select * from cp.`employee.json`";

//            String sql= "select convert_to(rl[1], 'JSON') list_col from cp.`store/json/input2.json`";
//            String sql= "select convert_from(convert_to(rl[1], 'JSON'), 'JSON') list_col from cp.`store/json/input2.json`";
//            String sql = "select employee_id as eid " +
//                    "                          , employee_id + position_id as eidpluspid " +
//                    "                   from cp.`employee.json` ";
//            employee_id<BIGINT(OPTIONAL)>,full_name<VARCHAR(OPTIONAL)>,first_name<VARCHAR(OPTIONAL)>,last_name<VARCHAR(OPTIONAL)>,position_id<BIGINT(OPTIONAL)>,position_title<VARCHAR(OPTIONAL)>,store_id<BIGINT(OPTIONAL)>,department_id<BIGINT(OPTIONAL)>,birth_date<VARCHAR(OPTIONAL)>,hire_date<VARCHAR(OPTIONAL)>,salary<FLOAT8(OPTIONAL)>,supervisor_id<BIGINT(OPTIONAL)>,education_level<VARCHAR(OPTIONAL)>,marital_status<VARCHAR(OPTIONAL)>,gender<VARCHAR(OPTIONAL)>,management_role<VARCHAR(OPTIONAL)>
//            1,Sheri Nowmer,Sheri,Nowmer,1,President,0,1,1961-08-26,1994-12-01 00:00:00.0,80000.0,0,Graduate Degree,S,F,Senior Management
//            String sql = "SELECT MAX(employee_id) as max_id " +
//                         " from cp.`employee.json` ";
//            String sql = "SELECT UPPER(CONCAT(first_name, '##')) as upper_name " +
//                    " from cp.`employee.json` ";

      String sql = "select BigIntValue, BooleanValue, DateValue, FloatValue, DoubleValue, IntegerValue, TimeValue, " +
                          " TimestampValue, IntervalYearValue, IntervalDayValue, IntervalSecondValue, " +
                          " 'Fq7gIk2x1FPI8nQUXG7SqBcXKx7ocIBa17CNC9KWtrlW7gZykOfHVQhipHs5C5DDAZWhb0EwxnEX16TA7Eb6HTC1 wMr3w0sPhVGRjfaDCSwx0U4c9BJCYCjnlwEcPo72DpwmkJeYSEmOP2A3YdKH7qH2YaOkVhSeEIuw5i4LaHx4zlbT7pzzv6Anf33I3yCVLoQG6KfP1t4UzTwEkWINSSRQdrNla9uCcpo658weoYJAVboBCEyJA3aP2mR526o746EM d9avydaP3O07FMgNa6p72ws8b3YHyWM69vuuPHXe9YO5lUE3TRzPXmwhoPlYYSvNM6ndcDqucj8ZftO77TN24EWoldMAPY29DgKril k2alzcMBEiyq0qFWpRecQ96af4wMbue7IuzFWWG7ZGsOBN0nKWYq6o1GWdP3P2fgeHsvi44SoRZr1SmUm54klJOLa2pQ4WA40br pW4ktjRpu TUKCs20geop0Wch0FHVozjdczOgCLxSCDWlinAYWSD drla26T0ubv8fY1uOf2SIJOjMuVcRbSYGIy51OFP40MXBI1csWtXXFpZmdLzViGP3Zw9Td3Nzt562LXOBBPw6sDRL9PvDfmuhNqF1u9foXRNxHwT7bqJV8IUoAFk4OMMaWW1ACEv9MtszVKHQKmwgEZpVfkk9XdwYp1VVAHLU0RXZqS5xTB CZRlsDVROD4jl9eZmjNepygPmnntAZb9AI6kwigVZDaNCLEyxNHFbp06p o7qjePkGZ7BHevqGip0RPJetoBoZipiIvoCC3dR2uCvkcBWqkCvs7rgNDeZkkF436R2Xs8pKsLjlkFSecOF27x8IMXt5087ApB5AAUh A8iSglXwLig9U9ORwrYbEFPy5UZw1LFqNIRO6ht6hdjBB7yyvLliLEdA0C7QDQ3ve60yx8Gzv8lhHJFVDWhDbaixeb6 ywO6VtExrdhhikQa12aSqyA1ZktMcCRNoLRjZJpZ3qTe7wuDn2e4IeCAMLAxmvukEmOQDzZDK4XYL84at8F M4mHKzC7Prpt0XASLMsYgiDqe7Jv69 KjfTo5cCAgDwlZZi8pi7ZqJKFcXwoTkX ZOjmNw0UJ6SQC1OOqU1iRFW6aIlAZn64XlBHxB4erpIZtYYDQyDd4lKExSw57VfI453hTUw3Fl7IwH kWAcW57qYIUAtj72TDPmYR93VWyE2prdmrZhsBi0L WRLKzZdP26qIhmth78pncOngJMU2JlyxsVjodvqSbR9vEyCnKMmrgiSGBWAxqDaX3CGXWpYK6iBrZAaNIkmdWlEwlJmZQbaJDuqCvcTATvwYM8PPhhKbC9a6kACa8sLyK4s4X QawveQCpUZMUnl6XoIqZNCb8rKi26 d BjXA0CUW9N79 j6W5 vB4QTtVfLfLR3 FZlMeMvwI7WdGF0pPYHcVBuIE LJA79Og7AbgMCUyBCbYGjAhMmqxuOFRwq0p3atbekuwqjc6SdtWJUsMp4NOrZaBmDVe mPUIMeHf1eujTT cf yVWO7skvwWy2Js yc3tVEP6wko6PDVgMZUwZfqHoiOKEtm6oohJpRP7Z23f13haSM1PB63o8f5eLb5we5nAU3KAxfohTod9PCQBAr3haSker rnnV KFyJDVdDnaVmt10ken1mAF0oISOkujgLO0I 5CfK5LxD0hSPczepOrP5uaGrzfahTnCLdMTwIg1JAgAHKFl7N4IFlI7kQe3cIdhtiT f1g5S4c6u1NaD13IOr60sMxcGIBiVu38aeAE1FwYK QlVMNhhxLy51zYUN4vX64bgqz9phG4LDR59PrFzaWmzIixVDVhxgGvOSvlLGYPShGuGQeFU78HPAI7NQ zkbQiyLDDDNXnTszJPVbU6WpeJiUEVigiPqH2AVAyI0cT3AQEruzSqfPMyZryincEUxTa9ZH6kNEYtRrUbMa3LVAZCLGNYlXB6P7wFpiB3XfSjO7aWYRTAcPTX6F2Rvr4nfmpp fzVFwwwH7h71ompZHk2hzWa3HCLNFf5BKZHu3t4AToOkEuP2tiPMUuPXOM1V6EDPQFjXrOZmOPqAbgMDehHOMMZxceSHiUouXDXTGoE6kFSkj65RWQPCAqkgHgy3NgRKy50YnNLah2zcc5MOIZwgEavZdLdvOymScqCp4tMD6YTH2v0K6H6WkDyDVLwdSwMcHxlhTgocz5e8tdGByPQoQYAweOLyuRRH4sXjFoyvtyBrPVD Kat7FPgrdw4w1O 3x3BvuRbLIXd9ViLQFGJ2Xc2 UHCR83zVDsYKXTnmEbF6KyiJZjcgynzlkliUmNr BFtIRR82RWy7IXnHDYL1MqNlXi9en11F9QiCYkrSPRkjgKxT nNKPiWDB8kSJJEKt95TGtxYjIREYEgnxMtFh 9G8CwvGwTmvkVT850biuQzd517kbrdTcr4KzY7ytkjZPhFv8mcJlydXuMWQGRdRkQTH7i3gumWOFe1sqtBRfulVPsauJJI3uCFaxMFW9MVxG uTiCg jfBC4zY0KkT1b1iHsrHCUFdZrkTmVfQK7JDVV1AO6CQHk1HprpuWnQ11c8mdfTKeDcc0OfgWKbmjVuYaddFM8W5Aj1QmaEwtETPmlSShNdWMyQuhLHQy61QhA9kEWoxKOrPFyjJ zDyXsceNyJyqbu7Yzt0OimaHfx1H jp7nWNPOUop5XbnW6pgwdqKL5abXc79Zn53687vAb1AYrPxxYeW3ZokQCB0eegmY2nSyOa6JcZ4EeTJ7P2eFHRUMvzXidw5yZI09cV9ji5gb8Y02KICZ3ffMrJIl8ETM21ZO4F84FEH0XXELqsnb8r7xtS3FkoPf2sLGudnedSyMEBloMT0dSK4FB3Ni4QwQVxGB1706sfTGOgOjoZj69kpi3xDbdNBdvzFYsAK8 OciiIclt8bpYPREsQa5yqDYRDBXYTxggoPmcepWxGmH9mKJhStuQvwtKtCeRP00m2PMZHQ2vw4JTOMIVm PLR6YeAk3PYQQat0DKWhXwYLBft4VYGRWsytysdXDXu5T4yUo5I1ztCMJSDkZ4lg4U4hjsT4GdjCnM1qW43KnmNIPv96xipUYOtBw7R4mFUGxnIYBOe tC093Ju5mazpz0qxfz4 sM4hZmAo6W9FLPCY4zl9sQu1xi6MIaEGbHwPrJ5PkiuN9VchXt62wLmAQ55y9DqvPfQh4iPRErqRhXgJj608xoMRMujmr53VBzG7VwoE8wnjLTnbXqOqgbW1o5aIIjMbABfImdwPC84vGEtZ8BlRMMEgjd7CqSbuRPrMYrBpafnJ8yzy8xWILRCKT9 IZGt8BSqP8Tdysy7rNcXoOTe2wF7IBPNamPqIakEv0B9PQ4SjcgTUQVSu7l574Vn1c41WtGs1sI7LKnUBdqvgBgaD D9oF9iLtUGiO7BSognuY7pCwjCoGYRg4GndpWCvadXCYy5 z0kuBlLC1jazKOU7O1xOWStsdBPdpOfno4yD1rlZyorneXZNdHz2w0f7J7GzV69booSlX8YyZ8pcgXhzve7hSyrJYGuH3nOkwK50CvyOMcD BcIpOYeAUk9PSgl8kregQc9WsH7LNRGyKM42nI4VgZ2crXTcNugQt3rcAkyyuk0gUYrb 3hxjv3T17tiWBQXYLlMdeTbr5RPmCQ718Egmg3t50KFHmMUMVV0jhXGUjFslRNKYGmTOSTThNTutpjShCUa6iJ4IfIaC1lgs9ov326eGz3e7jRynBCyjBZcR RUJv7kOz5TTa5a1u8MtC351GEQlunrqlce4FRNucqhNsR8Jm3Ha 42FzMHR0yD1SeUdPY2jDiilpfoSC3RiWzNo7rWdmOKrI9d9S8fk59WRCVJlGLhelxrkijjubp5k  d5G4p0eJEhYX2uWmpLgB42LDR8G8QT0ZZqyTUnojaJ7AxjiSHIF1qSU9SeVvmUazM92WRGqlkKhY8yrotUkNraPMVlammgGymUIYe7nInMysovxQHF2uCUCV8sHNiK3dnmtm6TxnDOalQXgHj fpYbMrIsllDP4szc6ke OWUQ3ANH4V9NKxi5Jivl0mFaAubgGxL9 q5XSNr0mXsqdWQXK3cjld6d R11Z7lBJKmeh uoFHwtOujxx2INqetor2bItqj5LAbk16JhyOJHW8lnkPHxinQtEL2SaoJhAC1aVGxrvZcgqPz6JecGd Sv9kDVhQ FB1fxP23dnmzHk8u6GPZjzHOAUzmq0bDa23WhLtuzHKU6envHsbRbF5K2 eq0an1QtZdmZbmyOOWK DK3VlfbQdczJ1hPktaNOlQnvXgzWRPvH7falw4f3h8crDQrmPpsULbdwkPoWf2pCz38A3NsUSrDWsszGXXHkbJsaI4OjuFZwoPl7rOyOKxjro0HuOBRfz jCkUEOdPOYrJOW6yeaXPAVERvBEivrBvT8y2BrqFKAtKsIo3zRui134YTHw9GKgDiI2OvGbaGNcIQfv7UhnYSLDKcjwKrlfBLc8t3Pw242ORPXeimitxTLWTDMHPCv42ZUglyQTE naxHaFT59YugopjB60nJ71NJ2PsOrwc8gxnjJJ4AuL2itssnw8ciedOUmQGcMwxZAa1eIXbSTruDUqpwZFDnaNA QUEnMK OKOO0iFJM6WEgVNQ5VqMUiUgZBcQZH8VYleOSAkXElF52KZ0QYJvjGQk7TRLyyAgnGTjya2FGnlQYR9w1DcQLc7rqqb1YsNnSsUV6QeDWWq1 DXHNIF0akpCErbhhhBVSbPIUnTfWUKfQTYNFHGl5MR1cCdMdsg1MUH83HVq76ONKUh0Sn4J8pb i0dkRE9rnjrvm8LzviFo9TFFsK42oVRGWwwfkBcfdsa8ef7OT V2feLEghFlxF0Cjt50Cgjf1CwJIowxhaTRJ WCvSjrwQGjiBrQZ841NUG1JI miWMbXJ1z7wPoQI2rBEXuczuTbv8V5kH0d2A9ZZIM4JJqwz8ISmbY7UtCE' NewCharacterValue " +
                          " from `dfs.data`.`alltypes_fixed_1MB.parquet`";

//            String sql = "SELECT employee_id + position_id as eidpluspid " +
//                            " from cp.`employee.json` ";
      //String sql = "select  from cp.`employee.json` limit 1 ";
//            String sql = "select employee_id + position_id as eidpluspid from cp.`employee.json` ";
      //String sql = "select employee_id as eid, employee_id as eid2, employee_id + position_id as eidpluspid from cp.`employee.json` ";

      client.queryBuilder().sql(sql).printCsv();
    }
  }
}