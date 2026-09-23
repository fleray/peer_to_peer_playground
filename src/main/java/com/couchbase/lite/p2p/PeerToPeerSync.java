package com.couchbase.lite.p2p;

import com.couchbase.lite.Collection;
import com.couchbase.lite.CollectionConfiguration;
import com.couchbase.lite.CouchbaseLite;
import com.couchbase.lite.CouchbaseLiteException;
import com.couchbase.lite.Database;
import com.couchbase.lite.DatabaseConfiguration;
import com.couchbase.lite.MutableDocument;
import com.couchbase.lite.ReplicatedDocument;
import com.couchbase.lite.Replicator;
import com.couchbase.lite.ReplicatorConfiguration;
import com.couchbase.lite.ReplicatorStatus;
import com.couchbase.lite.ReplicatorType;
import com.couchbase.lite.URLEndpoint;
import com.couchbase.lite.URLEndpointListener;
import com.couchbase.lite.URLEndpointListenerConfiguration;

import javax.jmdns.JmDNS;
import javax.jmdns.ServiceEvent;
import javax.jmdns.ServiceInfo;
import javax.jmdns.ServiceListener;

import java.io.File;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.URI;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Couchbase Lite 4.0 — Peer-to-Peer Sync Demo
 *
 * A single-class application demonstrating P2P replication between two peers
 * on the same local network. One instance runs as the SERVER (passive
 * listener),
 * the other as the CLIENT (active replicator). Peer discovery is automated
 * via DNS-SD (Bonjour) using JmDNS.
 *
 * Usage:
 * java -jar peer-to-peer-1.0.jar --mode server [--docs N] [--size N] [--bind IP]
 * java -jar peer-to-peer-1.0.jar --mode client [--docs N] [--size N] [--bind IP]
 *
 * Arguments:
 * --mode server | client (required)
 * --docs N Number of dummy documents to create locally (default: 10)
 * --size N Size of each dummy document in KB (default: 500)
 * --bind IP Local IP address used for DNS-SD (default: InetAddress.getLocalHost())
 */
public class PeerToPeerSync {

    // ─── Constants ───────────────────────────────────────────────────────
    private static final String DB_NAME = "p2p_demo_db";
    private static final int LISTENER_PORT = 55990;
    private static final String SERVICE_TYPE = "_cblite._tcp.local.";
    private static final String SERVICE_NAME = "CouchbaseLiteP2PDemo";
    private static final DateTimeFormatter TIME_FMT = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    // ─── Throughput tracking ─────────────────────────────────────────────
    private static final AtomicInteger totalDocsReceived = new AtomicInteger(0);
    private static final AtomicInteger totalDocsSent = new AtomicInteger(0);
    private static final AtomicLong replicationStartTime = new AtomicLong(0);
    private static final AtomicInteger totalDocsLocallyCreated = new AtomicInteger(0);

    // ─── DNS-SD interface (--bind) ───────────────────────────────────────
    private static String bindAddress = null;

    // ─── Entry point ─────────────────────────────────────────────────────
    public static void main(String[] args) throws Exception {
        // Parse CLI arguments
        String mode = null;
        int numDocs = 0;
        int docSizeKb = 500;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--mode":
                    if (i + 1 < args.length)
                        mode = args[++i].toLowerCase();
                    break;
                case "--docs":
                    if (i + 1 < args.length)
                        numDocs = Integer.parseInt(args[++i]);
                    break;
                case "--size":
                    if (i + 1 < args.length)
                        docSizeKb = Integer.parseInt(args[++i]);
                    break;
                case "--bind":
                    if (i + 1 < args.length)
                        bindAddress = args[++i];
                    break;
            }
        }

        if (mode == null || (!mode.equals("server") && !mode.equals("client"))) {
            printUsage();
            System.exit(1);
        }

        log("══════════════════════════════════════════════════════════════");
        log("  Couchbase Lite 4.0 — Peer-to-Peer Sync Demo");
        log("  Mode : " + mode.toUpperCase());
        log("  Docs : " + numDocs + " dummy documents will be created");
        log("  Size : " + docSizeKb + " KB per document");
        log("  Bind : " + (bindAddress != null ? bindAddress : "default (InetAddress.getLocalHost())"));
        log("══════════════════════════════════════════════════════════════");

        // Initialize Couchbase Lite
        CouchbaseLite.init();
        log("Couchbase Lite initialized.");

        // Create database in a mode-specific directory to avoid conflicts
        String dbDir = System.getProperty("java.io.tmpdir") + File.separator + "cblite_p2p_" + mode;
        new File(dbDir).mkdirs();
        log("CB Lite database created in directory: " + dbDir);
        DatabaseConfiguration dbConfig = new DatabaseConfiguration();
        dbConfig.setDirectory(dbDir);

        // Delete existing database for a clean start
        if (Database.exists(DB_NAME, new File(dbDir))) {
            Database.delete(DB_NAME, new File(dbDir));
            log("Deleted existing database for a clean start.");
        }

        Database database = new Database(DB_NAME, dbConfig);
        log("Database created at: " + dbDir + "/" + DB_NAME);

        // Get the default collection
        Collection collection = database.getDefaultCollection();
        if (collection == null) {
            log("ERROR: Could not get the default collection.");
            System.exit(1);
        }

        // Create dummy documents
        createDummyDocuments(collection, numDocs, mode, docSizeKb);

        // Run in the selected mode
        if (mode.equals("server")) {
            runServer(database, collection);
        } else {
            runClient(database, collection);
        }
    }

    // ═════════════════════════════════════════════════════════════════════
    // DOCUMENT CREATION
    // ═════════════════════════════════════════════════════════════════════

    private static void createDummyDocuments(Collection collection, int count, String mode, int docSizeKb)
            throws CouchbaseLiteException {
        log("Creating " + count + " dummy documents (" + docSizeKb + " KB each)...");
        long start = System.currentTimeMillis();

        String payloadStr = null;
        if (docSizeKb > 0) {
            char[] chars = new char[docSizeKb * 1024];
            for (int i = 0; i < chars.length; i++) {
                chars[i] = 'A';
            }
            payloadStr = new String(chars);
        }

        for (int i = 1; i <= count; i++) {
            MutableDocument doc = new MutableDocument(mode + "_doc_" + i);
            doc.setString("type", "demo");
            doc.setString("source", mode);
            doc.setString("name", "Document #" + i + " from " + mode.toUpperCase());
            doc.setString("created_at", Instant.now().toString());
            doc.setInt("index", i);
            doc.setDouble("value", Math.random() * 1000);
            doc.setString("description",
                    "This is a dummy document created by the " + mode + " peer for P2P sync testing.");

            if (payloadStr != null) {
                doc.setString("payload", payloadStr);
            }

            collection.save(doc);
        }

        long elapsed = System.currentTimeMillis() - start;
        totalDocsLocallyCreated.addAndGet(count);
        log("✓ Created " + count + " documents in " + elapsed + " ms");
        log("  Total documents in local DB: " + collection.getCount());
    }

    // ═════════════════════════════════════════════════════════════════════
    // SERVER MODE — Passive Peer (URLEndpointListener)
    // ═════════════════════════════════════════════════════════════════════

    private static void runServer(Database database, Collection collection) throws Exception {
        log("────────────────────────────────────────────────────────────");
        log("  Starting SERVER (Passive Peer / Listener)");
        log("────────────────────────────────────────────────────────────");

        // Configure the URL Endpoint Listener
        Set<Collection> collections = new HashSet<>();
        collections.add(collection);

        URLEndpointListenerConfiguration listenerConfig = new URLEndpointListenerConfiguration(collections);
        listenerConfig.setPort(LISTENER_PORT);
        listenerConfig.setDisableTls(true); // Disabled for demo simplicity

        URLEndpointListener listener = new URLEndpointListener(listenerConfig);

        // Add document change listener to track incoming replications
        collection.addChangeListener(change -> {
            List<String> docIds = change.getDocumentIDs();
            if (!docIds.isEmpty()) {
                totalDocsReceived.addAndGet(docIds.size());
                int totalCount = totalDocsReceived.get() + totalDocsSent.get();
                for (String docId : docIds) {
                    log("  📥 Received document: " + docId);
                }
                printThroughput(collection, totalCount);
            }
        });

        // Start the listener
        listener.start();
        replicationStartTime.set(System.currentTimeMillis());

        log("✓ Listener started on port " + LISTENER_PORT);
        log("  URLs: " + listener.getUrls());

        // Register the service via DNS-SD for peer discovery
        InetAddress localAddress = dnsSdAddress();
        log("  Local address: " + localAddress.getHostAddress());

        JmDNS jmdns = JmDNS.create(localAddress);
        ServiceInfo serviceInfo = ServiceInfo.create(
                SERVICE_TYPE,
                SERVICE_NAME,
                LISTENER_PORT,
                "Couchbase Lite P2P Demo Server");
        jmdns.registerService(serviceInfo);
        log("✓ DNS-SD service registered: " + SERVICE_NAME + " (" + SERVICE_TYPE + ")");

        log("");
        log("═══════════════════════════════════════════════════════════");
        log("  SERVER IS RUNNING — Waiting for client connections...");
        log("  Press Ctrl+C to stop.");
        log("═══════════════════════════════════════════════════════════");

        // Add shutdown hook for clean teardown
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log("");
            log("Shutting down...");
            jmdns.unregisterAllServices();
            try {
                jmdns.close();
            } catch (IOException e) {
                /* ignore */ }
            listener.stop();
            printFinalStats(collection, true);
            try {
                database.close();
            } catch (CouchbaseLiteException e) {
                /* ignore */ }
            log("Server stopped.");
        }));

        // Keep the server running
        new CountDownLatch(1).await();
    }

    // ═════════════════════════════════════════════════════════════════════
    // CLIENT MODE — Active Peer (Replicator)
    // ═════════════════════════════════════════════════════════════════════

    private static void runClient(Database database, Collection collection) throws Exception {
        log("────────────────────────────────────────────────────────────");
        log("  Starting CLIENT (Active Peer / Replicator)");
        log("────────────────────────────────────────────────────────────");
        log("  Searching for server via DNS-SD...");

        // Discover server via DNS-SD
        InetAddress localAddress = dnsSdAddress();
        JmDNS jmdns = JmDNS.create(localAddress);

        CountDownLatch discoveryLatch = new CountDownLatch(1);
        final String[] serverHost = new String[1];
        final int[] serverPort = new int[1];

        jmdns.addServiceListener(SERVICE_TYPE, new ServiceListener() {
            @Override
            public void serviceAdded(ServiceEvent event) {
                log("  🔍 Service discovered: " + event.getName());
                // Request service info to resolve it
                jmdns.requestServiceInfo(event.getType(), event.getName(), 5000);
            }

            @Override
            public void serviceResolved(ServiceEvent event) {
                ServiceInfo info = event.getInfo();
                if (info.getInet4Addresses().length > 0) {
                    serverHost[0] = info.getInet4Addresses()[0].getHostAddress();
                    serverPort[0] = info.getPort();
                    log("  ✓ Server resolved: " + serverHost[0] + ":" + serverPort[0]);
                    discoveryLatch.countDown();
                }
            }

            @Override
            public void serviceRemoved(ServiceEvent event) {
                log("  ⚠ Service removed: " + event.getName());
            }
        });

        log("  Waiting for server discovery...");
        discoveryLatch.await(); // Block until server is found

        // Build the replicator endpoint URL
        URI targetUri = new URI("ws://" + serverHost[0] + ":" + serverPort[0] + "/" + DB_NAME);
        log("  Target endpoint: " + targetUri);

        // Configure the replicator using the 4.0 API
        URLEndpoint endpoint = new URLEndpoint(targetUri);

        // Create a CollectionConfiguration for the default collection
        CollectionConfiguration collConfig = new CollectionConfiguration(collection);

        // Create the ReplicatorConfiguration with collection configs and endpoint
        ReplicatorConfiguration replConfig = new ReplicatorConfiguration(
                Collections.singleton(collConfig),
                endpoint);
        replConfig.setType(ReplicatorType.PUSH_AND_PULL);
        replConfig.setContinuous(true);
        replConfig.setAcceptOnlySelfSignedServerCertificate(false);

        Replicator replicator = new Replicator(replConfig);

        // Track replication start time
        replicationStartTime.set(System.currentTimeMillis());

        // Add status change listener
        replicator.addChangeListener(change -> {
            ReplicatorStatus status = change.getStatus();
            CouchbaseLiteException error = status.getError();
            String errorMsg = (error != null) ? " | Error: " + error.getMessage() : "";

            log("  ⟳ Replicator status: " + status.getActivityLevel()
                    + " | Progress: " + status.getProgress().getCompleted()
                    + "/" + status.getProgress().getTotal()
                    + errorMsg);
        });

        // Add document replication listener
        replicator.addDocumentReplicationListener(replication -> {
            boolean isPush = replication.isPush();
            List<ReplicatedDocument> docs = replication.getDocuments();

            if (isPush) {
                totalDocsSent.addAndGet(docs.size());
            } else {
                totalDocsReceived.addAndGet(docs.size());
            }
            int totalCount = totalDocsSent.get() + totalDocsReceived.get();

            for (ReplicatedDocument doc : docs) {
                String direction = isPush ? "📤 PUSH" : "📥 PULL";
                CouchbaseLiteException docError = doc.getError();
                String status = (docError != null)
                        ? " [ERROR: " + docError.getMessage() + "]"
                        : " [OK]";
                log("  " + direction + " " + doc.getID() + status);
            }

            printThroughput(collection, totalCount);
        });

        // Start replication
        replicator.start();
        log("✓ Replicator started (continuous push & pull)");

        log("");
        log("═══════════════════════════════════════════════════════════");
        log("  CLIENT IS RUNNING — Replicating with server...");
        log("  Press Ctrl+C to stop.");
        log("═══════════════════════════════════════════════════════════");

        // Add shutdown hook for clean teardown
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log("");
            log("Shutting down...");
            replicator.stop();
            try {
                jmdns.close();
            } catch (IOException e) {
                /* ignore */ }
            printFinalStats(collection, false);
            try {
                database.close();
            } catch (CouchbaseLiteException e) {
                /* ignore */ }
            log("Client stopped.");
        }));

        // Keep the client running
        new CountDownLatch(1).await();
    }

    // ═════════════════════════════════════════════════════════════════════
    // UTILITY METHODS
    // ═════════════════════════════════════════════════════════════════════

    /**
     * Note : for "server" side of the peering, "totalDocsSent" is always 0 because
     * there is no easy way of tracking it (because there is no
     * addDocumentReplicationListener equivalent in "server" mode).
     * 
     * So, to have reprensentative throughput for server sync, it is better to run
     * the server side program with 0 docs.
     * 
     * @param collection collection whose current document count is logged
     * @param totalDocs
     */
    private static void printThroughput(Collection collection, int totalDocs) {
        long elapsed = System.currentTimeMillis() - replicationStartTime.get();
        if (elapsed > 0) {
            double docsPerSec = (totalDocs * 1000.0) / elapsed;
            log(String.format("  📊 Throughput: %d docs replicated | total docs in DB: %d | %.1f docs/sec | elapsed: %.1f sec",
                    totalDocs, collection.getCount(), docsPerSec, elapsed / 1000.0));
        }
    }

    private static void printFinalStats(Collection collection, boolean isServer) {
        try {
            long elapsed = System.currentTimeMillis() - replicationStartTime.get();
            int received = totalDocsReceived.get();
            int sent = totalDocsSent.get();
            int localDocs = totalDocsLocallyCreated.get();

            log("");
            log("══════════════════════════════════════════════════════════");
            log("  FINAL STATISTICS");
            log("──────────────────────────────────────────────────────────");
            log("  Total docs in local DB     : " + collection.getCount());
            log("  Total docs locally created : " + localDocs);
            log("  Total docs replicated      :");
            log("    - Received               : " + received);
            if (isServer) {
                log("    - Sent                   : Unknown (tracked by active client)");
            } else {
                log("    - Sent                   : " + sent);
            }
            log("  Total time                 : " + String.format("%.1f", elapsed / 1000.0) + " sec");
            log("══════════════════════════════════════════════════════════");
        } catch (Exception e) {
            log("  Could not print final stats: " + e.getMessage());
        }
    }

    /**
     * Address JmDNS binds to. With several interfaces on the same LAN (e.g. Ethernet + Wi-Fi),
     * getLocalHost() may pick one the other peer's mDNS queries never reach: --bind selects
     * the interface explicitly (e.g. the Wi-Fi IP when the other peer is a phone on Wi-Fi).
     * The URLEndpointListener itself still listens on all interfaces.
     */
    private static InetAddress dnsSdAddress() throws IOException {
        if (bindAddress == null) {
            InetAddress a = InetAddress.getLocalHost();
            log("  DNS-SD address: " + a.getHostAddress() + " (default; use --bind <ip> to choose)");
            return a;
        }
        InetAddress a = InetAddress.getByName(bindAddress);
        NetworkInterface ni = NetworkInterface.getByInetAddress(a);
        if (ni == null) {
            log("ERROR: --bind " + bindAddress + " is not an address of this machine. Local IPv4 addresses:");
            for (NetworkInterface n : Collections.list(NetworkInterface.getNetworkInterfaces())) {
                if (!n.isUp() || n.isLoopback()) continue;
                for (InetAddress ia : Collections.list(n.getInetAddresses())) {
                    if (ia instanceof Inet4Address) log("    " + n.getName() + "  " + ia.getHostAddress());
                }
            }
            System.exit(1);
        }
        log("  DNS-SD address: " + a.getHostAddress() + " (interface " + ni.getName() + ", from --bind)");
        return a;
    }

    private static void log(String message) {
        System.out.println("[" + LocalDateTime.now().format(TIME_FMT) + "] " + message);
    }

    private static void printUsage() {
        System.out.println();
        System.out.println("Couchbase Lite 4.0 — Peer-to-Peer Sync Demo");
        System.out.println();
        System.out.println("Usage:");
        System.out.println("  java -jar peer-to-peer-1.0.jar --mode <server|client> [--docs N]");
        System.out.println();
        System.out.println("Arguments:");
        System.out.println("  --mode   server    Run as passive peer (listener)");
        System.out.println("           client    Run as active peer (replicator)");
        System.out.println("  --docs   N         Number of dummy documents to create locally (default: 10)");
        System.out.println("  --size   N         Size of each dummy document in KB (default: 500)");
        System.out.println("  --bind   IP        Local IP used for DNS-SD, e.g. the Wi-Fi IP (default: InetAddress.getLocalHost())");
        System.out.println();
        System.out.println("Example:");
        System.out.println("  Terminal 1:  java -jar peer-to-peer-1.0.jar --mode server --docs 100");
        System.out.println("  Terminal 2:  java -jar peer-to-peer-1.0.jar --mode client --docs 50");
        System.out.println();
    }
}
