package de.leipzig.htwk.gitrdf.sparql.query.api.controller;

import de.leipzig.htwk.gitrdf.sparql.query.api.documentation.GeneralInternalServerErrorApiResponse;
import de.leipzig.htwk.gitrdf.sparql.query.api.exception.BadRequestException;
import de.leipzig.htwk.gitrdf.sparql.query.api.model.request.QueryRequest;
import de.leipzig.htwk.gitrdf.sparql.query.api.response.error.BadRequestErrorResponse;
import de.leipzig.htwk.gitrdf.sparql.query.api.response.error.NotFoundErrorResponse;
import de.leipzig.htwk.gitrdf.sparql.query.service.impl.SparqlQueryServiceImpl;
import de.leipzig.htwk.gitrdf.sparql.query.utils.LongUtils;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.ExampleObject;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.Resource;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.SQLException;

@RestController
@RequiredArgsConstructor
@Slf4j
@RequestMapping(path = "/query-service/api/v1/github")
@Tag(name = "Query API")
public class QueryController {

    // for more infos regarding a SPARQL conform api: https://www.w3.org/TR/sparql11-protocol/
    // https://www.w3.org/2001/sw/DataAccess/rq23/#ask

    private final SparqlQueryServiceImpl sparqlQueryService;

    @Operation(
            summary = "Perform a SPARQL-Query on repository RDF data + statistics (excludes ratings for performance)",
            description = "Provide the query as a query parameter named 'query'. This endpoint loads the base Git repository RDF and statistics data, but excludes ratings to avoid timeouts.")
    @ApiResponse(
            responseCode = "200",
            description = "SPARQL-Query result in json",
            content = @Content(
                    mediaType = "application/sparql-results+json",
                    schema = @Schema(example = "{\"head\": {\"vars\": [ \"commit\" ]} ,\"results\": {\"bindings\": [{\"commit\": { \"type\": \"uri\" , \"value\": \"https://github.com/dotnet/core/commit/b0ec7806d47408656cb17230f8875cc9413064e0\"}}]}}")))
    @ApiResponse(
            responseCode = "400",
            description = "Bad Request",
            content = @Content(
                    mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(implementation = BadRequestErrorResponse.class),
                    examples = {
                            @ExampleObject(
                                    name = "Invalid id was specified",
                                    description = "Invalid id was specified",
                                    value = "{\"status\": \"Bad Request\", \"reason\": \"Invalid id 'blub' was given\", \"solution\": \"Provide a valid id. Example id: 55\"}"),
                            @ExampleObject(
                                    name = "Empty query string was given",
                                    description = "Empty query string was given",
                                    value = "{\"status\": \"Bad Request\", \"reason\": \"Empty SPARQL-Query given\", \"solution\": \"Provide a non empty and valid sparql-query\"}"),
                            @ExampleObject(
                                    name = "Github entry was not yet processed and rdf was not yet produced",
                                    description = "Github entry was not yet processed and rdf was not yet produced",
                                    value = "{\"status\": \"Bad Request\", \"reason\": \"The status of the github to rdf entry with the id '4' is not yet 'DONE'. The rdf was not yet produced.\", \"solution\": \"Provide an id for an github to rdf entry were the processing is already finished or wait till the processing for this particular entry is done\"}")}))
    @ApiResponse(
            responseCode = "404",
            description = "Not found",
            content = @Content(
                    mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(implementation = NotFoundErrorResponse.class),
                    examples = {
                            @ExampleObject(
                                    name = "No github to rdf entry found",
                                    description = "No github to rdf entry found",
                                    value = "{\"status\": \"Not found\", \"reason\": \"No github to rdf entry found for id '3'\", \"solution\": \"Provide an id for an existing github to rdf entry\"}")}))
    @GeneralInternalServerErrorApiResponse
    @GetMapping(value = "/rdf/query/{id}", produces = "application/sparql-results+json")
    public @ResponseBody Resource getResultOfGetQuery(
            @PathVariable("id") String id,
            @RequestParam("query") String query) throws SQLException, IOException {

        return getQueryJsonResultResponseFrom(id, query);
    }

    @Operation(
            summary = "Perform a SPARQL-Query on repository RDF data + statistics (excludes ratings for performance)",
            description = "Provide the query url encoded in the request body as 'query' field. This endpoint loads the base Git repository RDF and statistics data, but excludes ratings to avoid timeouts.")
    @ApiResponse(
            responseCode = "200",
            description = "SPARQL-Query result in json",
            content = @Content(
                    mediaType = "application/sparql-results+json",
                    schema = @Schema(example = "{\"head\": {\"vars\": [ \"commit\" ]} ,\"results\": {\"bindings\": [{\"commit\": { \"type\": \"uri\" , \"value\": \"https://github.com/dotnet/core/commit/b0ec7806d47408656cb17230f8875cc9413064e0\"}}]}}")))
    @ApiResponse(
            responseCode = "400",
            description = "Bad Request",
            content = @Content(
                    mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(implementation = BadRequestErrorResponse.class),
                    examples = {
                            @ExampleObject(
                                    name = "Invalid id was specified",
                                    description = "Invalid id was specified",
                                    value = "{\"status\": \"Bad Request\", \"reason\": \"Invalid id 'blub' was given\", \"solution\": \"Provide a valid id. Example id: 55\"}"),
                            @ExampleObject(
                                    name = "Empty query string was given",
                                    description = "Empty query string was given",
                                    value = "{\"status\": \"Bad Request\", \"reason\": \"Empty SPARQL-Query given\", \"solution\": \"Provide a non empty and valid sparql-query\"}"),
                            @ExampleObject(
                                    name = "Github entry was not yet processed and rdf was not yet produced",
                                    description = "Github entry was not yet processed and rdf was not yet produced",
                                    value = "{\"status\": \"Bad Request\", \"reason\": \"The status of the github to rdf entry with the id '4' is not yet 'DONE'. The rdf was not yet produced.\", \"solution\": \"Provide an id for an github to rdf entry were the processing is already finished or wait till the processing for this particular entry is done\"}")}))
    @ApiResponse(
            responseCode = "404",
            description = "Not found",
            content = @Content(
                    mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(implementation = NotFoundErrorResponse.class),
                    examples = {
                            @ExampleObject(
                                    name = "No github to rdf entry found",
                                    description = "No github to rdf entry found",
                                    value = "{\"status\": \"Not found\", \"reason\": \"No github to rdf entry found for id '3'\", \"solution\": \"Provide an id for an existing github to rdf entry\"}")}))
    @GeneralInternalServerErrorApiResponse
    @PostMapping(
            value = "/rdf/query/encoded/{id}",
            consumes = MediaType.APPLICATION_FORM_URLENCODED_VALUE,
            produces = "application/sparql-results+json")
    public @ResponseBody Resource getResultOfPostQueryFormEncoded(
            @PathVariable("id") String id,
            QueryRequest queryRequest) throws SQLException, IOException {

        return getQueryJsonResultResponseFrom(id, queryRequest.getQuery());
    }

    @Operation(
            summary = "Perform a SPARQL-Query on repository RDF data + statistics (excludes ratings for performance)",
            description = "Provide the query directly in the body. This endpoint loads the base Git repository RDF and statistics data, but excludes ratings to avoid timeouts.")
    @ApiResponse(
            responseCode = "200",
            description = "SPARQL-Query result in json",
            content = @Content(
                    mediaType = "application/sparql-results+json",
                    schema = @Schema(example = "{\"head\": {\"vars\": [ \"commit\" ]} ,\"results\": {\"bindings\": [{\"commit\": { \"type\": \"uri\" , \"value\": \"https://github.com/dotnet/core/commit/b0ec7806d47408656cb17230f8875cc9413064e0\"}}]}}")))
    @ApiResponse(
            responseCode = "400",
            description = "Bad Request",
            content = @Content(
                    mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(implementation = BadRequestErrorResponse.class),
                    examples = {
                            @ExampleObject(
                                    name = "Invalid id was specified",
                                    description = "Invalid id was specified",
                                    value = "{\"status\": \"Bad Request\", \"reason\": \"Invalid id 'blub' was given\", \"solution\": \"Provide a valid id. Example id: 55\"}"),
                            @ExampleObject(
                                    name = "Empty query string was given",
                                    description = "Empty query string was given",
                                    value = "{\"status\": \"Bad Request\", \"reason\": \"Empty SPARQL-Query given\", \"solution\": \"Provide a non empty and valid sparql-query\"}"),
                            @ExampleObject(
                                    name = "Github entry was not yet processed and rdf was not yet produced",
                                    description = "Github entry was not yet processed and rdf was not yet produced",
                                    value = "{\"status\": \"Bad Request\", \"reason\": \"The status of the github to rdf entry with the id '4' is not yet 'DONE'. The rdf was not yet produced.\", \"solution\": \"Provide an id for an github to rdf entry were the processing is already finished or wait till the processing for this particular entry is done\"}")}))
    @ApiResponse(
            responseCode = "404",
            description = "Not found",
            content = @Content(
                    mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(implementation = NotFoundErrorResponse.class),
                    examples = {
                            @ExampleObject(
                                    name = "No github to rdf entry found",
                                    description = "No github to rdf entry found",
                                    value = "{\"status\": \"Not found\", \"reason\": \"No github to rdf entry found for id '3'\", \"solution\": \"Provide an id for an existing github to rdf entry\"}")}))
    @GeneralInternalServerErrorApiResponse
    @PostMapping(
            value = "/rdf/query/{id}",
            consumes = "application/sparql-query",
            produces = "application/sparql-results+json")
    public @ResponseBody Resource getResultOfPostQueryDirectRequest(
            @PathVariable("id") String id,
            @RequestBody String query) throws SQLException, IOException {

        return getQueryJsonResultResponseFrom(id, query);
    }

    @Operation(
            summary = "Perform a SPARQL-Query on all data (repository + ratings + statistics) - may be slow",
            description = "Provide the query as a query parameter named 'query'. This endpoint loads repository RDF plus all ratings and statistics data. Use with caution for repositories with many ratings as it may timeout.")
    @ApiResponse(
            responseCode = "200",
            description = "SPARQL-Query result in json",
            content = @Content(
                    mediaType = "application/sparql-results+json",
                    schema = @Schema(example = "{\"head\": {\"vars\": [ \"commit\", \"rating\" ]} ,\"results\": {\"bindings\": [{\"commit\": { \"type\": \"uri\" , \"value\": \"https://github.com/dotnet/core/commit/b0ec7806d47408656cb17230f8875cc9413064e0\"}, \"rating\": {\"type\": \"literal\", \"value\": \"4.5\"}}]}}")))
    @ApiResponse(
            responseCode = "400",
            description = "Bad Request",
            content = @Content(
                    mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(implementation = BadRequestErrorResponse.class)))
    @ApiResponse(
            responseCode = "404",
            description = "Not found",
            content = @Content(
                    mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(implementation = NotFoundErrorResponse.class)))
    @GeneralInternalServerErrorApiResponse
    @GetMapping(value = "/rdf/query-combined/{id}", produces = "application/sparql-results+json")
    public @ResponseBody Resource getCombinedQueryResult(
            @PathVariable("id") String id,
            @RequestParam("query") String query) throws SQLException, IOException {

        return getCombinedQueryJsonResultResponseFrom(id, query);
    }

    @Operation(
            summary = "Perform a SPARQL-Query on ratings and statistics data only",
            description = "Provide the query as a query parameter named 'query'. This endpoint only loads ratings and statistics RDF data, excluding the base repository data.")
    @ApiResponse(
            responseCode = "200",
            description = "SPARQL-Query result in json",
            content = @Content(
                    mediaType = "application/sparql-results+json",
                    schema = @Schema(example = "{\"head\": {\"vars\": [ \"rating\" ]} ,\"results\": {\"bindings\": [{\"rating\": { \"type\": \"literal\" , \"value\": \"4.5\"}}]}}")))
    @ApiResponse(
            responseCode = "400",
            description = "Bad Request",
            content = @Content(
                    mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(implementation = BadRequestErrorResponse.class)))
    @ApiResponse(
            responseCode = "404",
            description = "Not found",
            content = @Content(
                    mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(implementation = NotFoundErrorResponse.class)))
    @GeneralInternalServerErrorApiResponse
    @GetMapping(value = "/rdf/query-analysis/{id}", produces = "application/sparql-results+json")
    public @ResponseBody Resource getAnalysisQueryResult(
            @PathVariable("id") String id,
            @RequestParam("query") String query) throws SQLException, IOException {

        return getAnalysisDataQueryJsonResultResponseFrom(id, query);
    }

    @Operation(
            summary = "Perform a SPARQL-Query on repository RDF data + expert data",
            description = "Provide the query directly in the body. This endpoint loads the base Git repository RDF and expert analysis data.")
    @ApiResponse(
            responseCode = "200",
            description = "SPARQL-Query result in json",
            content = @Content(
                    mediaType = "application/sparql-results+json",
                    schema = @Schema(example = "{\"head\": {\"vars\": [ \"commit\", \"expert\" ]} ,\"results\": {\"bindings\": [{\"commit\": { \"type\": \"uri\" , \"value\": \"https://github.com/dotnet/core/commit/b0ec7806d47408656cb17230f8875cc9413064e0\"}, \"expert\": {\"type\": \"literal\", \"value\": \"expert analysis data\"}}]}}")))
    @ApiResponse(
            responseCode = "400",
            description = "Bad Request",
            content = @Content(
                    mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(implementation = BadRequestErrorResponse.class)))
    @ApiResponse(
            responseCode = "404",
            description = "Not found",
            content = @Content(
                    mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(implementation = NotFoundErrorResponse.class)))
    @GeneralInternalServerErrorApiResponse
    @PostMapping(
            value = "/rdf/query-expert/{id}",
            consumes = "application/sparql-query",
            produces = "application/sparql-results+json")
    public @ResponseBody Resource getExpertQueryResult(
            @PathVariable("id") String id,
            @RequestBody String query) throws SQLException, IOException {

        return getExpertDataQueryJsonResultResponseFrom(id, query);
    }

    @Operation(
            summary = "Perform a SPARQL-Query on all data (repository + ratings + statistics + expert)",
            description = "Provide the query directly in the body. This endpoint loads all available RDF data including repository, ratings, statistics, and expert analysis data.")
    @ApiResponse(
            responseCode = "200",
            description = "SPARQL-Query result in json",
            content = @Content(
                    mediaType = "application/sparql-results+json",
                    schema = @Schema(example = "{\"head\": {\"vars\": [ \"commit\", \"rating\", \"expert\" ]} ,\"results\": {\"bindings\": [{\"commit\": { \"type\": \"uri\" , \"value\": \"https://github.com/dotnet/core/commit/b0ec7806d47408656cb17230f8875cc9413064e0\"}, \"rating\": {\"type\": \"literal\", \"value\": \"4.5\"}, \"expert\": {\"type\": \"literal\", \"value\": \"expert analysis data\"}}]}}")))
    @ApiResponse(
            responseCode = "400",
            description = "Bad Request",
            content = @Content(
                    mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(implementation = BadRequestErrorResponse.class)))
    @ApiResponse(
            responseCode = "404",
            description = "Not found",
            content = @Content(
                    mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(implementation = NotFoundErrorResponse.class)))
    @GeneralInternalServerErrorApiResponse
    @PostMapping(
            value = "/rdf/query-all/{id}",
            consumes = "application/sparql-query",
            produces = "application/sparql-results+json")
    public @ResponseBody Resource getAllQueryResult(
            @PathVariable("id") String id,
            @RequestBody String query) throws SQLException, IOException {

        return getAllDataQueryJsonResultResponseFrom(id, query);
    }

    private Resource getQueryJsonResultResponseFrom(String entityId, String query) throws SQLException, IOException {

        long longId = LongUtils.convertStringToLongIdOrThrowException(entityId);

        throwExceptionOnEmptyQueryString(query);

        File tempRdfQueryResultJsonFile = sparqlQueryService.performSparqlQuery(longId, query);

        return new InputStreamResource(new BufferedInputStream(new FileInputStream(tempRdfQueryResultJsonFile)));
    }

    private Resource getCombinedQueryJsonResultResponseFrom(String entityId, String query) throws SQLException, IOException {

        long longId = LongUtils.convertStringToLongIdOrThrowException(entityId);

        throwExceptionOnEmptyQueryString(query);

        File tempRdfQueryResultJsonFile = sparqlQueryService.performSparqlQueryCombined(longId, query);

        return new InputStreamResource(new BufferedInputStream(new FileInputStream(tempRdfQueryResultJsonFile)));
    }

    private Resource getAnalysisDataQueryJsonResultResponseFrom(String entityId, String query) throws SQLException, IOException {

        long longId = LongUtils.convertStringToLongIdOrThrowException(entityId);

        throwExceptionOnEmptyQueryString(query);

        File tempRdfQueryResultJsonFile = sparqlQueryService.performSparqlQueryAnalysisData(longId, query);

        return new InputStreamResource(new BufferedInputStream(new FileInputStream(tempRdfQueryResultJsonFile)));
    }

    private Resource getExpertDataQueryJsonResultResponseFrom(String entityId, String query) throws SQLException, IOException {

        long longId = LongUtils.convertStringToLongIdOrThrowException(entityId);

        throwExceptionOnEmptyQueryString(query);

        File tempRdfQueryResultJsonFile = sparqlQueryService.performSparqlQueryExpertData(longId, query);

        return new InputStreamResource(new BufferedInputStream(new FileInputStream(tempRdfQueryResultJsonFile)));
    }

    private Resource getAllDataQueryJsonResultResponseFrom(String entityId, String query) throws SQLException, IOException {

        long longId = LongUtils.convertStringToLongIdOrThrowException(entityId);

        throwExceptionOnEmptyQueryString(query);

        File tempRdfQueryResultJsonFile = sparqlQueryService.performSparqlQueryAllData(longId, query);

        return new InputStreamResource(new BufferedInputStream(new FileInputStream(tempRdfQueryResultJsonFile)));
    }

    private void throwExceptionOnEmptyQueryString(String query) {

        if (StringUtils.isBlank(query)) {
            throw BadRequestException.emptySparqlQueryString();
        }

    }

}
