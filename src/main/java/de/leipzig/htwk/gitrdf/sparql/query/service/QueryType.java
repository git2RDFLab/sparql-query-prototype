package de.leipzig.htwk.gitrdf.sparql.query.service;

public enum QueryType {
    BASIC("query"),
    COMBINED("query-combined"), 
    ANALYSIS("query-analysis"),
    EXPERT("query-expert"),
    ALL("query-all");
    
    private final String endpoint;
    
    QueryType(String endpoint) {
        this.endpoint = endpoint;
    }
    
    public String getEndpoint() {
        return endpoint;
    }
}