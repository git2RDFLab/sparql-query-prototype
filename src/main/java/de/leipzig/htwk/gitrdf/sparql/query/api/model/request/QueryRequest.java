package de.leipzig.htwk.gitrdf.sparql.query.api.model.request;

import lombok.Getter;

@Getter
public class QueryRequest {

    private final String query;

    public QueryRequest(String query) {
        this.query = query;
    }

}
