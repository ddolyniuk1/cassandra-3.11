package org.apache.cassandra.threesi.bifrost.services;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.cassandra.threesi.bifrost.facades.BifrostConsumerFacade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.*;
import java.lang.management.ManagementFactory;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("UnstableApiUsage")
public class BifrostImporterService implements BifrostImporterServiceMBean {
    private static final Logger logger = LoggerFactory.getLogger(CommitLogWatcherService.class);
    private static BifrostImporterService instance;
    
    private final BifrostConsumerFacade bifrostConsumerFacade = new BifrostConsumerFacade();
    
    // our cache of mutation hashes we previously imported
    private final Cache<String, Boolean> recentlyProcessedMutationsCache = CacheBuilder.newBuilder()
            .maximumSize(200000)
            .expireAfterWrite(1, TimeUnit.HOURS)
            .build();
    
    public static synchronized BifrostImporterService getInstance()
    {
        if (instance == null)
        {
            instance = new BifrostImporterService();
        }
        return instance;
    }
    public void register() throws MalformedObjectNameException, NotCompliantMBeanException, InstanceAlreadyExistsException, MBeanRegistrationException {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        mbs.registerMBean(this, new ObjectName("org.apache.cassandra.threesi:type=BifrostImporter"));
    }
    @Override
    @SuppressWarnings("UnusedReturnValue")
    public int importMutations(String filePath, int limit, String keyspaceFilter, String tableFilter)
    {
        return bifrostConsumerFacade.importMutations(filePath, limit, keyspaceFilter, tableFilter);
    }
    
    public void setIsRecentlyProcessedMutation(String mutationHash) {
        recentlyProcessedMutationsCache.put(mutationHash, true);
    }

    public Boolean getIsRecentlyProcessedMutation(String mutationHash) {
        return recentlyProcessedMutationsCache.getIfPresent(mutationHash);
    }
}
