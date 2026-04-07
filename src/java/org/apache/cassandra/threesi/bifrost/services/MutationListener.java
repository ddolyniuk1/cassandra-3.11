package org.apache.cassandra.threesi.bifrost.services;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.threesi.bifrost.facades.BifrostProducerFacade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class MutationListener
{
    private static final Logger logger = LoggerFactory.getLogger(CommitLogWatcherService.class);

    private static volatile MutationListener instance;
    private volatile boolean enabled = false;
    private final BifrostProducerFacade bifrostProducerFacade;

    public MutationListener(BifrostProducerFacade facade)
    {
        this.bifrostProducerFacade = facade;
    }

    public static MutationListener getInstance()
    {
        return instance;
    }

    public static void setInstance(MutationListener listener)
    {
        instance = listener;
    }
    public void disable() { enabled = false; }

    private final LinkedBlockingQueue<Mutation> mutationQueue =
            new LinkedBlockingQueue<>(100_000);
    private final ExecutorService processor = Executors.newFixedThreadPool(2);

    public void onMutation(Mutation mutation)
    {
        if (!enabled) return;
        if(mutation.isFromHub()) return;
        if (!DatabaseDescriptor.isBifrostMonitoredKeyspace(mutation.getKeyspaceName()))
            return;

        // non-blocking offer, drop if queue is full rather than
        // blocking the write path
        if (!mutationQueue.offer(mutation)) {
            logger.warn("Bifrost mutation queue full, dropping mutation");
        }
    }

    public void enable()
    {
        enabled = true;
        processor.submit(() -> {
            List<Mutation> batch = new ArrayList<>(256);

            while (enabled)
            {
                try
                {
                    batch.clear();

                    Mutation first = mutationQueue.poll(2, TimeUnit.SECONDS);
                    if (first != null)
                    {
                        batch.add(first);
                        mutationQueue.drainTo(batch, 255);
                    }

                    if (!batch.isEmpty())
                        bifrostProducerFacade.process(batch);

                }
                catch (InterruptedException e)
                {
                    Thread.currentThread().interrupt();
                    break;
                }
                catch (Exception e)
                {
                    logger.error("Error processing mutation batch", e);
                }
            }
        });
    }
}
