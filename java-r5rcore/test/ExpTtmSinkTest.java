import org.ipea.r5r.Utils.ExpTtmSink;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

/** expTTM codec round-trip + sink behavior. Python side verifies bytes. */
public class ExpTtmSinkTest {

    static void initSchema(String db) throws Exception {
        Class.forName("org.sqlite.JDBC");
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + db);
             Statement st = c.createStatement()) {
            st.execute("CREATE TABLE meta (k TEXT PRIMARY KEY, v TEXT NOT NULL)");
            st.execute("CREATE TABLE dest_index (idx INTEGER PRIMARY KEY, grid_id TEXT NOT NULL UNIQUE)");
            st.execute("CREATE TABLE origin_index (idx INTEGER PRIMARY KEY, grid_id TEXT NOT NULL UNIQUE)");
            st.execute("CREATE TABLE expttm_chunk (scenario_id INTEGER NOT NULL, origin_id TEXT NOT NULL," +
                       " chunk_id INTEGER NOT NULL, n_records INTEGER NOT NULL, codec INTEGER NOT NULL," +
                       " payload_version INTEGER NOT NULL, layout INTEGER NOT NULL, has_breakdown INTEGER NOT NULL," +
                       " payload BLOB NOT NULL, PRIMARY KEY (scenario_id, origin_id, chunk_id)) WITHOUT ROWID");
            st.execute("CREATE TABLE done (scenario_id INTEGER NOT NULL, origin_id TEXT NOT NULL," +
                       " elapsed_ms INTEGER, ts INTEGER, PRIMARY KEY (scenario_id, origin_id)) WITHOUT ROWID");
            st.execute("INSERT INTO meta VALUES ('schema_version','1')");
            st.execute("INSERT INTO meta VALUES ('max_trip_duration','120')");
        }
    }

    /** replicate PathBreakdown.roundTo1Place */
    static double r1(double v) { return Math.round(v * 10.0) / 10.0; }

    public static void main(String[] args) throws Exception {
        String db = args[0];
        initSchema(db);

        // Origin A: breakdown = TRUE
        ExpTtmSink.OriginCollector a = new ExpTtmSink.OriginCollector(true);
        // record 0: normal transit path, Korean UTF-8 route string
        a.add(3, 1, "08:00:00", "간선:TAGO_100|지선:TAGO_9401",
              r1(35.2333333), r1(4.5), r1(6.25), r1(20.0), r1(1.5), r1(3.0), 2);
        // record 1: same route string (dict reuse), draw 2
        a.add(3, 2, "08:00:00", "간선:TAGO_100|지선:TAGO_9401",
              r1(37.9), r1(4.5), r1(8.9), r1(20.0), r1(1.5), r1(3.0), 2);
        // record 2: direct walk supplement (empty departure in extract, then set) — here set
        a.add(7, 1, "08:01:00", "[WALK]",
              r1(88.7), 0, 0, 0, 0, 0, 0);
        // record 3: UNREACHABLE synthesized (departure set by loop, total = MAX_VALUE)
        a.add(9, 1, "08:02:00", "[WALK]",
              r1((double) Integer.MAX_VALUE), 0, 0, 0, 0, 0, 0);
        // record 4: empty departure edge (ARRIVE_BY recorded direct hypothetical)
        a.add(11, 1, "", "[WALK, BICYCLE]",
              r1(12.3), 0, 0, 0, 0, 0, 0);

        // Origin B: breakdown = FALSE
        ExpTtmSink.OriginCollector b = new ExpTtmSink.OriginCollector(false);
        b.add(0, 1, "09:30:00", "간선:TAGO_777", r1(15.05), 0, 0, 0, 0, 0, 0);
        b.add(1, 1, "09:31:00", "간선:TAGO_777", r1(0.0),  0, 0, 0, 0, 0, 0);

        try (ExpTtmSink sink = new ExpTtmSink(db, 4, 0)) {
            sink.submit(new ExpTtmSink.Item(5, "A", 0, a.size(), true,  11, a.encode(6)));
            sink.submit(new ExpTtmSink.Item(5, "B", 1, b.size(), false, 22, b.encode(6)));
        }
        System.out.println("write ok: A=" + a.size() + " records, B=" + b.size());

        // duplicate origin must fail loudly (plain INSERT, per-origin txn)
        boolean dupFailed = false;
        try (ExpTtmSink sink = new ExpTtmSink(db, 4, 0)) {
            sink.submit(new ExpTtmSink.Item(5, "A", 9, a.size(), true, 33, a.encode(6)));
            Thread.sleep(1500);
        } catch (Exception e) { dupFailed = true; }
        System.out.println("duplicate origin rejected=" + dupFailed);
    }
}
