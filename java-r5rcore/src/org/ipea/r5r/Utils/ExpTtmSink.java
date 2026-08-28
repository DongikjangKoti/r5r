package org.ipea.r5r.Utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Single-writer SQLite sink for EXPANDED travel time matrix results (koti-db-sink).
 *
 * Differences from TtmSink (strategy doc §3.6):
 *  - variable records per origin -> columnar payload with an EMBEDDED per-origin
 *    route dictionary. Payloads are fully self-contained: no global dict table,
 *    so crash consistency between dict and payload cannot arise by construction.
 *  - one origin = ONE transaction (chunk rows + done row), regardless of chunks.
 *    Partial origins are structurally impossible (review M8).
 *  - all time fields are stored as tenths of a minute (uint16). This is LOSSLESS:
 *    PathBreakdown getters already round to 1 decimal (roundTo1Place), and
 *    R-side tenths/10 reproduces bit-identical doubles.
 *
 * Schema owner is R (expttm_create_schema in ttm_db.R); this class validates only.
 */
public final class ExpTtmSink implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ExpTtmSink.class);

    public static final int PAYLOAD_VERSION = 1;
    public static final int LAYOUT_COLUMNAR = 1;
    /** layout 2: dict key = routes \u001F firstBoardStop \u001F lastAlightStop */
    public static final int LAYOUT_COLUMNAR_STOPS = 2;
    public static final char DICT_SEP = '\u001f';
    public static final String EXPECTED_SCHEMA_VERSION = "1";

    public static final int SENT_U16 = 0xFFFF;        // unreachable total / empty departure
    public static final long SENT_U32 = 0xFFFFFFFFL;

    /** Hard v1 limits — fail fast instead of silently mis-encoding (chunking comes later if hit). */
    public static final int MAX_RECORDS_PER_ORIGIN = 16_000_000;
    public static final int MAX_LOCAL_ROUTES = 65_535;

    public record Item(int scenarioId, String originId, int runIdx,
                       int nRecords, boolean breakdown, long elapsedMs, byte[] payload) {}

    private static final Item POISON = new Item(-1, null, -1, 0, false, 0, new byte[0]);

    private final BlockingQueue<Item> queue;
    private final Thread writer;
    private volatile Exception failure;
    private volatile boolean stopped = false;

    public ExpTtmSink(String dbPath, int capacity, int walAutoCheckpoint)
            throws SQLException, ClassNotFoundException {
        Class.forName("org.sqlite.JDBC");
        this.queue = new ArrayBlockingQueue<>(capacity);

        Connection conn = null;
        try {
            conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
            try (Statement st = conn.createStatement()) {
                st.execute("PRAGMA journal_mode=WAL");
                st.execute("PRAGMA synchronous=NORMAL");
                if (walAutoCheckpoint > 0)
                    st.execute("PRAGMA wal_autocheckpoint=" + walAutoCheckpoint);
                try (ResultSet rs = st.executeQuery("PRAGMA wal_autocheckpoint")) {
                    if (rs.next()) LOG.info("ExpTtmSink writer connection: wal_autocheckpoint={}",
                                            rs.getInt(1));
                }
                try (ResultSet rs = st.executeQuery(
                        "SELECT count(*) FROM sqlite_master WHERE type='table' AND name IN " +
                        "('meta','dest_index','origin_index','expttm_chunk','done')")) {
                    rs.next();
                    if (rs.getInt(1) != 5)
                        throw new IllegalStateException(
                            "expTTM DB is not initialized or schema is incompatible: " + dbPath);
                }
                try (ResultSet rs = st.executeQuery(
                        "SELECT v FROM meta WHERE k='schema_version'")) {
                    if (!rs.next() || !EXPECTED_SCHEMA_VERSION.equals(rs.getString(1)))
                        throw new IllegalStateException(
                            "expTTM DB schema_version mismatch (expected " +
                            EXPECTED_SCHEMA_VERSION + "): " + dbPath);
                }
            }
            conn.setAutoCommit(false);

            final Connection writerConn = conn;
            this.writer = new Thread(() -> loop(writerConn), "expttm-writer");
            this.writer.start();
        } catch (Exception e) {
            if (conn != null) { try { conn.close(); } catch (SQLException ignored) {} }
            if (e instanceof SQLException se) throw se;
            if (e instanceof ClassNotFoundException ce) throw ce;
            if (e instanceof RuntimeException re) throw re;
            throw new RuntimeException(e);
        }
    }

    private void checkWriter() {
        if (failure != null) throw new IllegalStateException("expttm writer failed", failure);
        if (stopped)         throw new IllegalStateException("expttm writer already stopped");
    }

    public void submit(Item it) throws InterruptedException {
        while (true) {
            checkWriter();
            if (queue.offer(it, 1, TimeUnit.SECONDS)) return;
        }
    }

    public int queueSize() { return queue.size(); }

    private void loop(Connection conn) {
        try (PreparedStatement a = conn.prepareStatement(
                 "INSERT INTO expttm_chunk (scenario_id, origin_id, chunk_id, n_records," +
                 " codec, payload_version, layout, has_breakdown, payload)" +
                 " VALUES (?,?,0,?,1,?,?,?,?)");            // v1: single chunk (id 0)
             PreparedStatement b = conn.prepareStatement(
                 "INSERT INTO done (scenario_id, origin_id, elapsed_ms, ts)" +
                 " VALUES (?,?,?,?)")) {
            while (true) {
                Item it = queue.take();
                if (it == POISON) break;

                // ONE origin = ONE transaction (chunk + done), committed immediately.
                a.setInt(1, it.scenarioId());   a.setString(2, it.originId());
                a.setInt(3, it.nRecords());     a.setInt(4, PAYLOAD_VERSION);
                a.setInt(5, LAYOUT_COLUMNAR_STOPS);   a.setInt(6, it.breakdown() ? 1 : 0);
                a.setBytes(7, it.payload());    a.executeUpdate();

                b.setInt(1, it.scenarioId());   b.setString(2, it.originId());
                b.setLong(3, it.elapsedMs());   b.setLong(4, System.currentTimeMillis());
                b.executeUpdate();

                conn.commit();
            }
            conn.commit();
        } catch (Exception e) {
            failure = e;
            try { conn.rollback(); } catch (SQLException ignored) {}
        } finally {
            stopped = true;
            try { conn.close(); } catch (SQLException ignored) {}
        }
    }

    @Override
    public void close() throws Exception {
        while (failure == null && !stopped) {
            if (queue.offer(POISON, 1, TimeUnit.SECONDS)) break;
        }
        writer.join();
        if (failure != null) throw failure;
    }

    // ------------------------------------------------------------------
    // Per-origin collector: accumulated on a ROUTING thread, encoded there
    // (compression stays parallel), then submitted as one Item.
    // ------------------------------------------------------------------
    public static final class OriginCollector {
        private final boolean breakdown;
        private int n = 0;
        private int[] toIdx      = new int[1024];
        private long[] depSec    = new long[1024];   // seconds-of-day; SENT_U32 = empty
        private short[] draw     = new short[1024];
        private int[] totalT     = new int[1024];    // tenths; SENT_U16 = unreachable
        private int[] routeCode  = new int[1024];    // local per-origin code
        private int[] accessT, waitT, rideT, transferT, egressT;
        private short[] nRides;
        private final Map<String, Integer> dict = new HashMap<>();
        private final List<String> dictList = new ArrayList<>();

        public OriginCollector(boolean breakdown) {
            this.breakdown = breakdown;
            if (breakdown) {
                accessT = new int[1024]; waitT = new int[1024]; rideT = new int[1024];
                transferT = new int[1024]; egressT = new int[1024]; nRides = new short[1024];
            }
        }

        private void grow() {
            int cap = toIdx.length * 2;
            toIdx = java.util.Arrays.copyOf(toIdx, cap);
            depSec = java.util.Arrays.copyOf(depSec, cap);
            draw = java.util.Arrays.copyOf(draw, cap);
            totalT = java.util.Arrays.copyOf(totalT, cap);
            routeCode = java.util.Arrays.copyOf(routeCode, cap);
            if (breakdown) {
                accessT = java.util.Arrays.copyOf(accessT, cap);
                waitT = java.util.Arrays.copyOf(waitT, cap);
                rideT = java.util.Arrays.copyOf(rideT, cap);
                transferT = java.util.Arrays.copyOf(transferT, cap);
                egressT = java.util.Arrays.copyOf(egressT, cap);
                nRides = java.util.Arrays.copyOf(nRides, cap);
            }
        }

        /** minutes (already rounded to 1 decimal by PathBreakdown) -> lossless tenths. */
        public static int tenths(double minutes1dp) {
            if (minutes1dp >= 6553.4 || minutes1dp < 0) return SENT_U16;  // incl. Integer.MAX_VALUE sentinel
            return (int) Math.round(minutes1dp * 10.0);
        }

        /** "HH:MM:SS" -> seconds-of-day; empty/null -> SENT_U32. */
        public static long depSeconds(String hms) {
            if (hms == null || hms.isEmpty()) return SENT_U32;
            int h = Integer.parseInt(hms, 0, 2, 10);
            int m = Integer.parseInt(hms, 3, 5, 10);
            int sec = Integer.parseInt(hms, 6, 8, 10);
            return h * 3600L + m * 60L + sec;
        }

        public void add(int destIdx, int drawNumber, String departureTime, String routes,
                        String firstBoardStop, String lastAlightStop,
                        double totalTime1dp,
                        double access1dp, double wait1dp, double ride1dp,
                        double transfer1dp, double egress1dp, int rides) {
            if (n == MAX_RECORDS_PER_ORIGIN)
                throw new IllegalStateException("expTTM origin exceeds " + MAX_RECORDS_PER_ORIGIN +
                        " records - chunking not yet implemented (v1 fail-fast)");
            if (n == toIdx.length) grow();
            toIdx[n] = destIdx;
            depSec[n] = depSeconds(departureTime);
            draw[n] = (short) drawNumber;
            totalT[n] = tenths(totalTime1dp);
            String key = (routes == null ? "" : routes)
                    + DICT_SEP + (firstBoardStop == null ? "" : firstBoardStop)
                    + DICT_SEP + (lastAlightStop == null ? "" : lastAlightStop);
            Integer code = dict.get(key);
            if (code == null) {
                if (dictList.size() == MAX_LOCAL_ROUTES)
                    throw new IllegalStateException("expTTM origin exceeds " + MAX_LOCAL_ROUTES +
                            " distinct route+stop sequences (v1 fail-fast)");
                code = dictList.size();
                dict.put(key, code);
                dictList.add(key);
            }
            routeCode[n] = code;
            if (breakdown) {
                accessT[n] = tenths(access1dp);   waitT[n] = tenths(wait1dp);
                rideT[n] = tenths(ride1dp);       transferT[n] = tenths(transfer1dp);
                egressT[n] = tenths(egress1dp);   nRides[n] = (short) rides;
            }
            n++;
        }

        public int size() { return n; }

        /**
         * Payload (layout=2, little-endian, gzip):
         *   header : n_records u32, n_dict u32, has_breakdown u8
         *   columns: to_idx u32[n], dep_sec u32[n], draw u8[n],
         *            total_tenths u16[n], route_code u16[n]
         *   if breakdown: access,wait,ride,transfer,egress u16[n] (tenths), n_rides u8[n]
         *   dict   : per code: len u16 + UTF-8 bytes; content =
         *            routes \u001F firstBoardStop \u001F lastAlightStop
         */
        public byte[] encode(int level) {
            int dictBytes = 0;
            byte[][] dictUtf = new byte[dictList.size()][];
            for (int i = 0; i < dictList.size(); i++) {
                dictUtf[i] = dictList.get(i).getBytes(StandardCharsets.UTF_8);
                if (dictUtf[i].length > 65535)
                    throw new IllegalStateException("route sequence string exceeds 65535 bytes");
                dictBytes += 2 + dictUtf[i].length;
            }
            int fixed = 9 + n * (4 + 4 + 1 + 2 + 2) + (breakdown ? n * (2 * 5 + 1) : 0);
            ByteBuffer buf = ByteBuffer.allocate(fixed + dictBytes).order(ByteOrder.LITTLE_ENDIAN);
            buf.putInt(n);
            buf.putInt(dictList.size());
            buf.put((byte) (breakdown ? 1 : 0));
            for (int i = 0; i < n; i++) buf.putInt(toIdx[i]);
            for (int i = 0; i < n; i++) buf.putInt((int) depSec[i]);      // u32 (SENT_U32 wraps to -1)
            for (int i = 0; i < n; i++) buf.put((byte) draw[i]);
            for (int i = 0; i < n; i++) buf.putShort((short) totalT[i]);
            for (int i = 0; i < n; i++) buf.putShort((short) routeCode[i]);
            if (breakdown) {
                for (int i = 0; i < n; i++) buf.putShort((short) accessT[i]);
                for (int i = 0; i < n; i++) buf.putShort((short) waitT[i]);
                for (int i = 0; i < n; i++) buf.putShort((short) rideT[i]);
                for (int i = 0; i < n; i++) buf.putShort((short) transferT[i]);
                for (int i = 0; i < n; i++) buf.putShort((short) egressT[i]);
                for (int i = 0; i < n; i++) buf.put((byte) nRides[i]);
            }
            for (byte[] u : dictUtf) { buf.putShort((short) u.length); buf.put(u); }

            ByteArrayOutputStream out = new ByteArrayOutputStream(1 << 16);
            try (LevelGZIP gz = new LevelGZIP(out, level)) {
                gz.write(buf.array(), 0, buf.position());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return out.toByteArray();
        }
    }

    private static final class LevelGZIP extends GZIPOutputStream {
        LevelGZIP(OutputStream out, int level) throws IOException {
            super(out, 1 << 16);
            def.setLevel(level);
        }
    }
}
