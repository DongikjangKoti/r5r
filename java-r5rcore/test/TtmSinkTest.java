import org.ipea.r5r.Utils.TtmSink;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

/** §6-1 codec round-trip + basic sink behavior. Python side verifies bytes. */
public class TtmSinkTest {

    static void initSchema(String db) throws Exception {
        Class.forName("org.sqlite.JDBC");
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + db);
             Statement st = c.createStatement()) {
            st.execute("CREATE TABLE meta (k TEXT PRIMARY KEY, v TEXT NOT NULL)");
            st.execute("CREATE TABLE dest_index (idx INTEGER PRIMARY KEY, grid_id TEXT NOT NULL UNIQUE)");
            st.execute("CREATE TABLE origin_index (idx INTEGER PRIMARY KEY, grid_id TEXT NOT NULL UNIQUE)");
            st.execute("CREATE TABLE ttm (scenario_id INTEGER NOT NULL, origin_id TEXT NOT NULL," +
                       " run_idx INTEGER, n_pct INTEGER NOT NULL, n_dest INTEGER NOT NULL," +
                       " width INTEGER NOT NULL, codec INTEGER NOT NULL," +
                       " payload_version INTEGER NOT NULL, layout INTEGER NOT NULL," +
                       " n_reached INTEGER NOT NULL, payload BLOB NOT NULL," +
                       " PRIMARY KEY (scenario_id, origin_id)) WITHOUT ROWID");
            st.execute("CREATE TABLE done (scenario_id INTEGER NOT NULL, origin_id TEXT NOT NULL," +
                       " elapsed_ms INTEGER, ts INTEGER," +
                       " PRIMARY KEY (scenario_id, origin_id)) WITHOUT ROWID");
            st.execute("INSERT INTO meta VALUES ('schema_version','1')");
        }
    }

    public static void main(String[] args) throws Exception {
        String db = args[0];
        initSchema(db);

        final int UNREACHED = Integer.MAX_VALUE;
        int cutoff = 120;                       // uint16 (>254 not needed; also test uint8 below)

        // Origin A: unsorted percentiles (75,25) — row filter case:
        //   dest0: p75=130 (>cutoff, FIRST pct) , p25=90 (<=cutoff)  -> row absent -> BOTH sentinel
        //   dest1: p75=100, p25=60                                   -> both kept
        //   dest2: p75=UNREACHED, p25=UNREACHED                      -> sentinel
        //   dest3: p75=120, p25=121 (cell layer on 2nd pct)          -> 120, sentinel
        //   dest4: p75=0,   p25=5                                    -> kept (0 valid)
        int[][] a = {
            {130, 100, UNREACHED, 120, 0},      // first percentile block (p75 as given)
            { 90,  60, UNREACHED, 121, 5}       // second (p25)
        };
        byte[] payloadA = TtmSink.encode(a, 5, 2, cutoff, 6);
        int reachedA = TtmSink.countReached(a[0], cutoff);   // expects 4 (dest1..4)

        // Origin B: single percentile, uint8, cutoff 200
        int[][] b = { {0, 200, 201, UNREACHED, 42} };
        byte[] payloadB = TtmSink.encode(b, 5, 1, 200, 6);
        int reachedB = TtmSink.countReached(b[0], 200);      // expects 4

        try (TtmSink sink = new TtmSink(db, 4, 2, 0)) {
            sink.submit(new TtmSink.Item(7, "A", 0, 2, 5, 2, reachedA, 11, payloadA));
            sink.submit(new TtmSink.Item(7, "B", 1, 1, 5, 1, reachedB, 22, payloadB));
        }
        System.out.println("write ok: reachedA=" + reachedA + " reachedB=" + reachedB);

        // duplicate INSERT must fail loudly (plain INSERT, no OR REPLACE)
        boolean dupFailed = false;
        try (TtmSink sink = new TtmSink(db, 4, 1, 0)) {
            sink.submit(new TtmSink.Item(7, "A", 9, 2, 5, 2, reachedA, 33, payloadA));
            Thread.sleep(1500);
            try { sink.submit(new TtmSink.Item(7, "X", 9, 2, 5, 2, 0, 0, payloadA)); }
            catch (IllegalStateException e) { dupFailed = true; }
        } catch (Exception e) { dupFailed = true; }   // close() rethrows the writer failure
        System.out.println("duplicate INSERT rejected=" + dupFailed);

        // cutoff/width fail-fast
        boolean widthFail = false;
        try { TtmSink.encode(b, 5, 1, 255, 6); } catch (IllegalArgumentException e) { widthFail = true; }
        System.out.println("width fail-fast=" + widthFail);
    }
}
