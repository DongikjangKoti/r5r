package org.ipea.r5r.Utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Single-writer SQLite sink for dense travel time matrix results.
 *
 * Design (see R5_TTM_DB_구축전략.md v6):
 *  - N routing threads encode+compress and submit() items; ONE writer thread inserts.
 *  - ttm row + done row share the same transaction (atomic checkpointing).
 *  - Bounded queue provides backpressure; producers never block forever if the
 *    writer dies (timeout offer loop + state check).
 *  - Schema is owned by the R side (create_schema in ttm_db.R). This class only
 *    VALIDATES the schema and fails fast on an uninitialized/incompatible DB.
 */
public final class TtmSink implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(TtmSink.class);

    public static final int PAYLOAD_VERSION = 1;
    public static final int LAYOUT_PCT_MAJOR = 1;
    public static final String EXPECTED_SCHEMA_VERSION = "1";   // keep in sync with R create_schema()

    public record Item(int scenarioId, String originId, int runIdx,
                       int nPct, int nDest, int width,
                       int nReached, long elapsedMs, byte[] payload) {}

    private static final Item POISON = new Item(-1, null, -1, 0, 0, 0, 0, 0, new byte[0]);

    private final BlockingQueue<Item> queue;
    private final Thread writer;
    private volatile Exception failure;
    private volatile boolean stopped = false;

    public TtmSink(String dbPath, int capacity, int commitEvery, int walAutoCheckpoint)
            throws SQLException, ClassNotFoundException {
        Class.forName("org.sqlite.JDBC");                      // explicit: rJava classloader safety
        this.queue = new ArrayBlockingQueue<>(capacity);       // bounded -> backpressure

        Connection conn = null;
        try {
            conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
            try (Statement st = conn.createStatement()) {
                st.execute("PRAGMA journal_mode=WAL");
                st.execute("PRAGMA synchronous=NORMAL");
                if (walAutoCheckpoint > 0)                     // connection-scoped setting
                    st.execute("PRAGMA wal_autocheckpoint=" + walAutoCheckpoint);
                try (ResultSet rs = st.executeQuery("PRAGMA wal_autocheckpoint")) {
                    if (rs.next()) LOG.info("TtmSink writer connection: wal_autocheckpoint={}",
                                            rs.getInt(1));
                }

                // Schema owner is R; validate only, never create/repair here.
                try (ResultSet rs = st.executeQuery(
                        "SELECT count(*) FROM sqlite_master WHERE type='table' AND name IN " +
                        "('meta','dest_index','origin_index','ttm','done')")) {
                    rs.next();
                    if (rs.getInt(1) != 5)
                        throw new IllegalStateException(
                            "TTM DB is not initialized or schema is incompatible: " + dbPath);
                }
                try (ResultSet rs = st.executeQuery(
                        "SELECT v FROM meta WHERE k='schema_version'")) {
                    if (!rs.next() || !EXPECTED_SCHEMA_VERSION.equals(rs.getString(1)))
                        throw new IllegalStateException(
                            "TTM DB schema_version mismatch (expected " +
                            EXPECTED_SCHEMA_VERSION + "): " + dbPath);
                }
            }
            conn.setAutoCommit(false);

            final Connection writerConn = conn;
            this.writer = new Thread(() -> loop(writerConn, commitEvery), "ttm-writer");
            this.writer.start();
        } catch (Exception e) {
            // Constructor failure must not leak the connection: if we throw here,
            // Utils.ttmSink is never assigned, so run()'s finally cannot close it.
            if (conn != null) { try { conn.close(); } catch (SQLException ignored) {} }
            if (e instanceof SQLException se) throw se;
            if (e instanceof ClassNotFoundException ce) throw ce;
            if (e instanceof RuntimeException re) throw re;
            throw new RuntimeException(e);
        }
    }

    private void checkWriter() {
        if (failure != null) throw new IllegalStateException("ttm writer failed", failure);
        if (stopped)         throw new IllegalStateException("ttm writer already stopped");
    }

    /** Called from routing threads. Converts a dead writer into an exception within ~1s. */
    public void submit(Item it) throws InterruptedException {
        while (true) {
            checkWriter();
            if (queue.offer(it, 1, TimeUnit.SECONDS)) return;
        }
    }

    /** Current queue occupancy, for bottleneck diagnostics. */
    public int queueSize() { return queue.size(); }

    private void loop(Connection conn, int commitEvery) {
        try (PreparedStatement a = conn.prepareStatement(
                 "INSERT INTO ttm (scenario_id, origin_id, run_idx, n_pct, n_dest," +
                 " width, codec, payload_version, layout, n_reached, payload)" +
                 " VALUES (?,?,?,?,?,?,1,?,?,?,?)");
             PreparedStatement b = conn.prepareStatement(
                 "INSERT INTO done (scenario_id, origin_id, elapsed_ms, ts)" +
                 " VALUES (?,?,?,?)")) {
            int n = 0;
            while (true) {
                Item it = queue.take();
                if (it == POISON) break;

                a.setInt(1, it.scenarioId());   a.setString(2, it.originId());
                a.setInt(3, it.runIdx());       a.setInt(4, it.nPct());
                a.setInt(5, it.nDest());        a.setInt(6, it.width());
                /* codec = 1 literal */         a.setInt(7, PAYLOAD_VERSION);
                a.setInt(8, LAYOUT_PCT_MAJOR);  a.setInt(9, it.nReached());
                a.setBytes(10, it.payload());   a.executeUpdate();

                b.setInt(1, it.scenarioId());   b.setString(2, it.originId());
                b.setLong(3, it.elapsedMs());   b.setLong(4, System.currentTimeMillis());
                b.executeUpdate();

                if (++n % commitEvery == 0) conn.commit();
            }
            conn.commit();
        } catch (Exception e) {
            failure = e;
            try { conn.rollback(); } catch (SQLException ignored) {}  // discard current txn only
        } finally {
            stopped = true;
            try { conn.close(); } catch (SQLException ignored) {}
        }
    }

    /** GZIPOutputStream defaults to level 6; expose the level. */
    private static final class LevelGZIP extends GZIPOutputStream {
        LevelGZIP(OutputStream out, int level) throws IOException {
            super(out, 1 << 16);
            def.setLevel(level);
        }
    }

    /**
     * dense int[percentile][destination] -> compressed bytes.
     * Replicates existing r5r output semantics in two layers (strategy doc §3.3):
     *   row layer  : values[0][d] > cutoff  -> ALL percentiles of d become sentinel
     *                (TravelTimeMatrixComputer.java:131 row-existence filter; note that
     *                 percentiles are NOT sorted by the R side, so this is not redundant)
     *   cell layer : values[p][d] > cutoff or UNREACHED -> that cell becomes sentinel
     *                (per-percentile cutoff at :143)
     * Record order is percentile-major (layout=1), little-endian.
     * Runs on routing threads so compression is parallel.
     */
    public static byte[] encode(int[][] values, int nDest, int width,
                                int cutoffMinutes, int level) {
        final int sentinel = (width == 1) ? 255 : 65535;
        if (cutoffMinutes >= sentinel)
            throw new IllegalArgumentException("cutoff " + cutoffMinutes +
                " does not fit width " + width);
        final int nPct = values.length;
        ByteBuffer buf = ByteBuffer.allocate(nPct * nDest * width)
                                   .order(ByteOrder.LITTLE_ENDIAN);
        for (int p = 0; p < nPct; p++) {
            for (int d = 0; d < nDest; d++) {
                boolean rowExists = values[0][d] >= 0 && values[0][d] <= cutoffMinutes;
                int v = values[p][d];                          // minutes; UNREACHED = MAX_VALUE
                if (!rowExists || v < 0 || v > cutoffMinutes) v = sentinel;
                if (width == 1) buf.put((byte) v); else buf.putShort((short) v);
            }
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream(1 << 17);
        try (LevelGZIP gz = new LevelGZIP(out, level)) {
            gz.write(buf.array());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return out.toByteArray();
    }

    /** Count of destinations whose FIRST percentile is within cutoff (== legacy CSV row count). */
    public static int countReached(int[] firstPercentile, int cutoffMinutes) {
        int reached = 0;
        for (int v : firstPercentile) if (v >= 0 && v <= cutoffMinutes) reached++;
        return reached;
    }

    /** Poison injection also refuses to block forever; guarantees final commit on clean close. */
    @Override
    public void close() throws Exception {
        while (failure == null && !stopped) {
            if (queue.offer(POISON, 1, TimeUnit.SECONDS)) break;
        }
        writer.join();
        if (failure != null) throw failure;
    }
}
