package de.leipzig.htwk.gitrdf.sparql.query.service;

import org.apache.jena.rdf.model.Model;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.time.LocalDateTime;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Getter
@RequiredArgsConstructor
public class StoreEntry {
    private final long orderId;
    private final QueryType queryType;
    private final Model model;
    private final LocalDateTime createdAt;
    private volatile LocalDateTime lastAccessedAt;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    
    public StoreEntry(long orderId, QueryType queryType, Model model) {
        this.orderId = orderId;
        this.queryType = queryType;
        this.model = model;
        this.createdAt = LocalDateTime.now();
        this.lastAccessedAt = LocalDateTime.now();
    }
    
    public void updateLastAccessed() {
        this.lastAccessedAt = LocalDateTime.now();
    }
    
    public boolean isExpired(int timeoutMinutes) {
        return LocalDateTime.now().isAfter(lastAccessedAt.plusMinutes(timeoutMinutes));
    }
    
    public String getStoreKey() {
        return orderId + ":" + queryType.name();
    }
}