package de.leipzig.htwk.gitrdf.sparql.query.service;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.jena.rdf.model.Model;
import org.springframework.stereotype.Service;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class TripleStoreManager {
    
    private static final int MAX_STORES = 8;
    private static final int TIMEOUT_MINUTES = 10;
    
    private final Map<String, StoreEntry> stores = new ConcurrentHashMap<>();
    private final ReentrantReadWriteLock globalLock = new ReentrantReadWriteLock();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    
    public TripleStoreManager() {
        // Start cleanup task every minute
        scheduler.scheduleWithFixedDelay(this::cleanupExpiredStores, 1, 1, TimeUnit.MINUTES);
    }
    
    public Optional<Model> getStore(long orderId, QueryType queryType) {
        String storeKey = createStoreKey(orderId, queryType);
        
        globalLock.readLock().lock();
        try {
            StoreEntry entry = stores.get(storeKey);
            if (entry != null && !entry.isExpired(TIMEOUT_MINUTES)) {
                entry.updateLastAccessed();
                log.debug("Retrieved existing store for order {} with query type {}", orderId, queryType);
                return Optional.of(entry.getModel());
            }
        } finally {
            globalLock.readLock().unlock();
        }
        
        return Optional.empty();
    }
    
    public void putStore(long orderId, QueryType queryType, Model model) {
        String storeKey = createStoreKey(orderId, queryType);
        
        globalLock.writeLock().lock();
        try {
            // Check if we need to make space
            if (stores.size() >= MAX_STORES && !stores.containsKey(storeKey)) {
                evictOldestStore();
            }
            
            StoreEntry entry = new StoreEntry(orderId, queryType, model);
            stores.put(storeKey, entry);
            
            log.info("Stored new triple store for order {} with query type {} (total stores: {})", 
                     orderId, queryType, stores.size());
            
        } finally {
            globalLock.writeLock().unlock();
        }
    }
    
    public void removeStore(long orderId, QueryType queryType) {
        String storeKey = createStoreKey(orderId, queryType);
        
        globalLock.writeLock().lock();
        try {
            StoreEntry removed = stores.remove(storeKey);
            if (removed != null) {
                log.info("Manually removed store for order {} with query type {}", orderId, queryType);
            }
        } finally {
            globalLock.writeLock().unlock();
        }
    }
    
    private void evictOldestStore() {
        StoreEntry oldestEntry = null;
        String oldestKey = null;
        LocalDateTime oldestTime = LocalDateTime.now();
        
        for (Map.Entry<String, StoreEntry> entry : stores.entrySet()) {
            StoreEntry storeEntry = entry.getValue();
            if (storeEntry.getLastAccessedAt().isBefore(oldestTime)) {
                oldestTime = storeEntry.getLastAccessedAt();
                oldestEntry = storeEntry;
                oldestKey = entry.getKey();
            }
        }
        
        if (oldestKey != null && oldestEntry != null) {
            stores.remove(oldestKey);
            log.info("Evicted oldest store for order {} with query type {} (last accessed: {})", 
                     oldestEntry.getOrderId(), oldestEntry.getQueryType(), oldestEntry.getLastAccessedAt());
        }
    }
    
    private void cleanupExpiredStores() {
        globalLock.writeLock().lock();
        try {
            stores.entrySet().removeIf(entry -> {
                StoreEntry storeEntry = entry.getValue();
                if (storeEntry.isExpired(TIMEOUT_MINUTES)) {
                    log.info("Cleaned up expired store for order {} with query type {} (expired at: {})", 
                             storeEntry.getOrderId(), storeEntry.getQueryType(), 
                             storeEntry.getLastAccessedAt().plusMinutes(TIMEOUT_MINUTES));
                    return true;
                }
                return false;
            });
        } finally {
            globalLock.writeLock().unlock();
        }
    }
    
    private String createStoreKey(long orderId, QueryType queryType) {
        return orderId + ":" + queryType.name();
    }
    
    public int getCurrentStoreCount() {
        return stores.size();
    }
    
    public void shutdown() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        
        globalLock.writeLock().lock();
        try {
            stores.clear();
        } finally {
            globalLock.writeLock().unlock();
        }
    }
}