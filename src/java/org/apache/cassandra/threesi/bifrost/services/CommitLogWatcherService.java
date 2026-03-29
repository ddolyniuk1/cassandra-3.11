package org.apache.cassandra.threesi.bifrost.services;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.commitlog.CommitLogDescriptor;
import org.apache.cassandra.db.commitlog.CommitLogReadHandler;
import org.apache.cassandra.db.commitlog.CommitLogReader;
import org.apache.cassandra.threesi.bifrost.facades.BifrostProducerFacade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class CommitLogWatcherService {
    private static final Logger logger = LoggerFactory.getLogger(CommitLogWatcherService.class);

    private static CommitLogWatcherService instance;
    private ExecutorService watcherExecutorService;
    private volatile boolean running = false;

    private final BifrostProducerFacade bifrostProducerFacade = new BifrostProducerFacade();
    private ScheduledExecutorService cleanupExecutorService;

    private CommitLogWatcherService() {
    }

    public static synchronized CommitLogWatcherService getInstance() {
        if (instance == null) {
            instance = new CommitLogWatcherService();
        }
        return instance;
    }

    /**
     * Begins our watcher and cleanup
     */
    public void start() {
        if (!DatabaseDescriptor.isBifrostEnabled()) {
            logger.debug("Bifrost is disabled in configuration.");
            return;
        }

        if (running) {
            logger.info("Bifrost Service already running");
            return;
        }

        logger.info("Starting Bifrost Service...");

        try {
            String cdcRawDirectory = DatabaseDescriptor.getCDCLogLocation();
            if (cdcRawDirectory == null || cdcRawDirectory.trim().isEmpty()) {
                logger.error("Data Bridge enabled but cdc_bridge_raw_directory not configured");
                return;
            }
            
            Path cdcRawPath = Paths.get(cdcRawDirectory);
            
            // initialize our data producers
            bifrostProducerFacade.start();
            
            // start the watcher thread to observe our cdc_raw directory for changes
            // using daemon mode (per Java docs: The Java Virtual Machine exits when the only threads 
            // running are all daemon threads.)
            watcherExecutorService = Executors.newSingleThreadExecutor(r -> {
                Thread t = new Thread(r, "data-bridge-watcher");
                t.setDaemon(true);
                return t;
            });
            
            // start our cleanup scheduler thread in daemon mode
            cleanupExecutorService = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "data-bridge-cleanup");
                t.setDaemon(true);
                return t;
            });
            
            running = true;
            watcherExecutorService.submit(() -> startWatching(cdcRawPath));
            cleanupExecutorService.scheduleWithFixedDelay(
                    () -> periodicCleanup(cdcRawPath),
                    5, 5, TimeUnit.MINUTES
            );
            logger.info("Data Bridge Service started successfully, monitoring: {}", cdcRawDirectory);
        } catch (Exception e) {
            logger.error("Failed to start Data Bridge Service", e);
            running = false;
        }
    }
 
    public void stop() {
        if (!running)
            return;

        logger.info("Stopping Data Bridge Service...");
        running = false;

        if (watcherExecutorService != null) {
            watcherExecutorService.shutdown();

            try {
                boolean terminated = watcherExecutorService.awaitTermination(10, TimeUnit.SECONDS);
                if (!terminated) {
                    watcherExecutorService.shutdownNow();
                }
            } catch (Exception exception) {
                logger.error("Failed to call shutdownNow on watcher service", exception);
            }
        }

        if (cleanupExecutorService != null) {
            cleanupExecutorService.shutdown();
        }

        logger.info("Data Bridge Service stopped");
    }

    private void startWatching(Path cdcDirectory) {
        WatchService watchService = null;
        try {
            if (!Files.exists(cdcDirectory)) {
                Files.createDirectories(cdcDirectory);
            }

            processExistingFiles(cdcDirectory);

            watchService = FileSystems.getDefault().newWatchService();
            cdcDirectory.register(watchService,
                    StandardWatchEventKinds.ENTRY_CREATE,
                    StandardWatchEventKinds.ENTRY_MODIFY);

            while (running) {
                // poll with timeout instead of blocking take()
                WatchKey key = watchService.poll(2, TimeUnit.SECONDS);

                if (key != null) {
                    for (WatchEvent<?> event : key.pollEvents()) {
                        WatchEvent.Kind<?> kind = event.kind();
                        if (kind == StandardWatchEventKinds.ENTRY_CREATE
                                || kind == StandardWatchEventKinds.ENTRY_MODIFY) {
                            Path file = cdcDirectory.resolve((Path) event.context());
                            if (file.toString().endsWith(".log")) {
                                processIfReady(file.toFile());
                            }
                            // also trigger on idx file changes
                            if (file.toString().endsWith(".idx")) {
                                File logFile = new File(
                                        file.toString().replace("_cdc.idx", ".log"));
                                if (logFile.exists()) {
                                    processIfReady(logFile);
                                }
                            }
                        }
                    }
                    key.reset();
                } else {
                    // no events poll the directory anyway as a fallback
                    processExistingFiles(cdcDirectory);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.info("Watcher interrupted, shutting down");
        } catch (Exception e) {
            logger.error("Error in watcher", e);
        } finally {
            if (watchService != null) {
                try { watchService.close(); } catch (IOException e) { /* ignore */ }
            }
        }
    }

    private void processIfReady(File logFile) {
        if (!running || hasBeenProcessed(logFile)) return;

        // Check for Cassandra's COMPLETED marker
        File cdcIdx = new File(logFile.getAbsolutePath().replace(".log", "_cdc.idx"));
        if (!cdcIdx.exists()) return;

        try {
            String content = new String(Files.readAllBytes(cdcIdx.toPath())).trim();
            if (!content.contains("COMPLETED")) return;
        } catch (IOException e) {
            return;
        }

        processCommitLogFile(logFile);
    }

    private void processExistingFiles(Path cdcPath) {
        try (Stream<Path> files = Files.list(cdcPath)) {
            files
                    .filter(path -> path.toString().endsWith(".log"))
                    .filter(path -> !hasBeenProcessed(path.toFile()))
                    .filter(path -> isReadyForProcessing(path.toFile()))
                    .forEach(path -> {
                        try {
                            processCommitLogFile(path.toFile());
                        } catch (Exception e) {
                            logger.error("Error processing CDC file: {}", path, e);
                        }
                    });
        } catch (IOException e) {
            logger.error("Error listing CDC directory", e);
        }
    }

    private boolean isReadyForProcessing(File logFile) {
        File idxFile = new File(logFile.getAbsolutePath().replace(".log", "_cdc.idx"));
        if (!idxFile.exists()) {
            return false;
        }
        try {
            String content = new String(Files.readAllBytes(idxFile.toPath())).trim();
            return content.contains("COMPLETED");
        } catch (IOException e) {
            return false;
        }
    }
    
    private void processCommitLogFile(File logFile) {
        if (!running)
            return;

        logger.debug("Processing CDC file: {}", logFile.getName());

        try {
            CommitLogReader reader = new CommitLogReader();
            reader.readCommitLogSegment(new CommitLogReadHandler() {
                @Override
                public boolean shouldSkipSegmentOnError(CommitLogReadException e) throws IOException {
                    logger.warn("Skipping CDC segment on error: {}", e.getMessage());
                    return true;
                }

                @Override
                public void handleUnrecoverableError(CommitLogReadException e) throws IOException {
                    logger.error("Unrecoverable error in CDC commit log: {}", e.getMessage());
                }

                @Override
                public void handleMutation(Mutation mutation, int size, int entryLocation, CommitLogDescriptor descriptor) {
                    try {
                        processMutation(mutation);
                    } catch (Exception e) {
                        logger.error("Error inserting CDC mutation to SQLite", e);
                    }
                }
            }, logFile, false);

            // if the log is not held by another process delete it immediately
            if (canSafelyDelete(logFile)) {
                tryDeleteAllFiles(logFile);
            } else { 
                // simply create an index indicating we have already processed the file for future cleanup
                createProcessedMarker(logFile);
            }
        } catch (Exception e) {
            logger.error("Error processing CDC file: {}", logFile.getName(), e);
        }
    }

    private void tryDeleteAllFiles(File logFile) {
        try {
            Files.deleteIfExists(logFile.toPath());
            Files.deleteIfExists(Paths.get(logFile.getAbsolutePath().replace(".log", "_cdc.idx")));
            Files.deleteIfExists(Paths.get(getProcessedMarkerPath(logFile)));
        } catch (IOException e) {
            logger.error("Error deleting files for: {}", logFile.getName(), e);
        }
    }

    private boolean isActiveCommitLog(File logFile) {
        long maxAge = TimeUnit.MINUTES.toMillis(5);
        return (System.currentTimeMillis() - logFile.lastModified()) < maxAge;
    }

    private boolean canSafelyDelete(File logFile) {
        if (isActiveCommitLog(logFile)) {
            return false;
        }

        try (RandomAccessFile raf = new RandomAccessFile(logFile, "rw")) {
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    /**
     * Callback for when a mutation is observed in our commit log
     * @param mutation
     */
    private void processMutation(Mutation mutation) {
        if (mutation == null)
            return;

        String keyspace = mutation.getKeyspaceName();

        if (!DatabaseDescriptor.isBifrostMonitoredKeyspace(keyspace)) return;

//        mutation
//                .getPartitionUpdates()
//                .forEach(partitionUpdate -> bifrostProducerFacade.process(partitionUpdate, mutation));
//    
    }

    /**
     * Applies cleanup on already processed commit logs, to prevent Cassandra
     * from filling the cdc_raw directory.
     * @param cdcDirectory
     */
    private void periodicCleanup(Path cdcDirectory) {
        try (Stream<Path> files = Files.list(cdcDirectory)) {
            files
                .filter(path -> path.toString().endsWith(".log"))
                .filter(path -> hasBeenProcessed(path.toFile()))
                .forEach(path -> {
                    File file = path.toFile();
                    if (canSafelyDelete(file)) {
                        tryDeleteAllFiles(file);
                    }
                });
        } catch (Exception e) {
            logger.error("Error in periodic cleanup", e);
        }
    }
    
    private static String getIdxFilePath(File logFile) {
        return logFile.getAbsolutePath().replace(".log", ".idx");
    }

    private static String getProcessedMarkerPath(File logFile) {
        return logFile.getAbsolutePath().replace(".log", ".processed");
    }

    private boolean hasBeenProcessed(File logFile) {
        return new File(getProcessedMarkerPath(logFile)).exists();
    }

    private void createProcessedMarker(File logFile) {
        try {
            File marker = new File(getProcessedMarkerPath(logFile));
            if (!marker.exists()) {
                String content = "processed_by_cdc_bridge:" + System.currentTimeMillis();
                Files.write(marker.toPath(), content.getBytes());
            }
        } catch (IOException e) {
            logger.debug("Could not create processed marker for: {}", logFile.getName(), e);
        }
    }
}