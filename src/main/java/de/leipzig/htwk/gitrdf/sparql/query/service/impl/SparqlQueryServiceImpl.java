package de.leipzig.htwk.gitrdf.sparql.query.service.impl;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.sql.Blob;
import java.sql.SQLException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import de.leipzig.htwk.gitrdf.database.common.entity.GithubRepositoryOrderEntity;
import de.leipzig.htwk.gitrdf.database.common.entity.GithubRepositoryOrderAnalysisEntity;
import de.leipzig.htwk.gitrdf.database.common.entity.enums.AnalysisType;
import de.leipzig.htwk.gitrdf.database.common.entity.enums.GitRepositoryOrderStatus;
import de.leipzig.htwk.gitrdf.database.common.entity.lob.GithubRepositoryOrderEntityLobs;
import de.leipzig.htwk.gitrdf.database.common.repository.GithubRepositoryOrderAnalysisRepository;
import de.leipzig.htwk.gitrdf.sparql.query.api.exception.BadRequestException;
import de.leipzig.htwk.gitrdf.sparql.query.api.exception.NotFoundException;
import de.leipzig.htwk.gitrdf.sparql.query.service.QueryType;
import de.leipzig.htwk.gitrdf.sparql.query.service.TripleStoreManager;
import jakarta.persistence.EntityManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@RequiredArgsConstructor
@Slf4j
public class SparqlQueryServiceImpl {

    private final int FIFTY_MEGABYTE = 1024 * 1024 * 50;
    private final int BUFFER_SIZE = FIFTY_MEGABYTE;
    private final String TURTLE_FORMAT = "TURTLE";

    private final EntityManager entityManager;
    private final GithubRepositoryOrderAnalysisRepository analysisRepository;
    private final TripleStoreManager tripleStoreManager;


    @Transactional(rollbackFor = { SQLException.class, IOException.class })
    public File performSparqlQuery(long entryId, String queryString) throws SQLException, IOException {
        File resultRdfFile = File.createTempFile("json-result-rdf-file", "json");

        // Check if model exists in store manager first
        Model combinedModel = tripleStoreManager.getStore(entryId, QueryType.BASIC).orElse(null);
        
        if (combinedModel == null) {
            // Load from database if not in store manager
            GithubRepositoryOrderEntityLobs githubRepositoryOrderEntityLobs = entityManager
                    .find(GithubRepositoryOrderEntityLobs.class, entryId);

            if (githubRepositoryOrderEntityLobs == null) {
                throw NotFoundException.githubEntryNotFound(entryId);
            }

            GithubRepositoryOrderEntity githubRepositoryOrderEntity = githubRepositoryOrderEntityLobs.getOrderEntity();

            if (!githubRepositoryOrderEntity.getStatus().equals(GitRepositoryOrderStatus.DONE)) {
                throw BadRequestException.githubToRdfConversionNotDone(entryId);
            }

            try {
                Model baseModel = loadBaseGitRdfModel(githubRepositoryOrderEntityLobs);
                Model statisticsModel = loadAnalysisRdfModel(entryId, AnalysisType.STATISTIC);
                combinedModel = ModelFactory.createUnion(baseModel, statisticsModel);
                
                // Store the combined model for future use
                tripleStoreManager.putStore(entryId, QueryType.BASIC, combinedModel);
                
                log.info("Loaded and cached new model for order {} with BASIC query type", entryId);
            } catch (Exception e) {
                log.error("SPARQL query failed for order {}: {}", entryId, e.getMessage());
                throw e;
            }
        } else {
            log.debug("Using cached model for order {} with BASIC query type", entryId);
        }

        try {
            Query rdfQuery = QueryFactory.create(queryString);
            executeQuery(rdfQuery, combinedModel, resultRdfFile);
        } catch (Exception e) {
            log.error("SPARQL query execution failed for order {}: {}", entryId, e.getMessage());
            throw e;
        }

        return resultRdfFile;
    }

    @Transactional(rollbackFor = { SQLException.class, IOException.class })
    public File performSparqlQueryCombined(long entryId, String queryString) throws SQLException, IOException {
        File resultRdfFile = File.createTempFile("json-result-rdf-file", "json");

        // Check if model exists in store manager first
        Model combinedModel = tripleStoreManager.getStore(entryId, QueryType.COMBINED).orElse(null);
        
        if (combinedModel == null) {
            // Load from database if not in store manager
            GithubRepositoryOrderEntityLobs githubRepositoryOrderEntityLobs = entityManager
                    .find(GithubRepositoryOrderEntityLobs.class, entryId);

            if (githubRepositoryOrderEntityLobs == null) {
                throw NotFoundException.githubEntryNotFound(entryId);
            }

            GithubRepositoryOrderEntity githubRepositoryOrderEntity = githubRepositoryOrderEntityLobs.getOrderEntity();

            if (!githubRepositoryOrderEntity.getStatus().equals(GitRepositoryOrderStatus.DONE)) {
                throw BadRequestException.githubToRdfConversionNotDone(entryId);
            }

            try {
                Model baseModel = loadBaseGitRdfModel(githubRepositoryOrderEntityLobs);
                Model ratingsModel = loadAnalysisRdfModel(entryId, AnalysisType.RATING);
                Model statisticsModel = loadAnalysisRdfModel(entryId, AnalysisType.STATISTIC);
                combinedModel = ModelFactory.createUnion(baseModel, ratingsModel);
                combinedModel = ModelFactory.createUnion(combinedModel, statisticsModel);
                
                // Store the combined model for future use
                tripleStoreManager.putStore(entryId, QueryType.COMBINED, combinedModel);
                
                log.info("Loaded and cached new model for order {} with COMBINED query type", entryId);
            } catch (Exception e) {
                log.error("SPARQL query failed for order {}: {}", entryId, e.getMessage());
                throw e;
            }
        } else {
            log.debug("Using cached model for order {} with COMBINED query type", entryId);
        }

        try {
            Query rdfQuery = QueryFactory.create(queryString);
            executeQuery(rdfQuery, combinedModel, resultRdfFile);
        } catch (Exception e) {
            log.error("SPARQL query execution failed for order {}: {}", entryId, e.getMessage());
            throw e;
        }

        return resultRdfFile;
    }

    @Transactional(rollbackFor = { SQLException.class, IOException.class })
    public File performSparqlQueryAnalysisData(long entryId, String queryString) throws SQLException, IOException {
        File resultRdfFile = File.createTempFile("json-result-rdf-file", "json");

        // Check if model exists in store manager first
        Model analysisModel = tripleStoreManager.getStore(entryId, QueryType.ANALYSIS).orElse(null);
        
        if (analysisModel == null) {
            // Load from database if not in store manager
            GithubRepositoryOrderEntityLobs githubRepositoryOrderEntityLobs = entityManager
                    .find(GithubRepositoryOrderEntityLobs.class, entryId);

            if (githubRepositoryOrderEntityLobs == null) {
                throw NotFoundException.githubEntryNotFound(entryId);
            }

            GithubRepositoryOrderEntity githubRepositoryOrderEntity = githubRepositoryOrderEntityLobs.getOrderEntity();

            if (!githubRepositoryOrderEntity.getStatus().equals(GitRepositoryOrderStatus.DONE)) {
                throw BadRequestException.githubToRdfConversionNotDone(entryId);
            }

            try {
                Model ratingsModel = loadAnalysisRdfModel(entryId, AnalysisType.RATING);
                Model statisticsModel = loadAnalysisRdfModel(entryId, AnalysisType.STATISTIC);
                analysisModel = ModelFactory.createUnion(ratingsModel, statisticsModel);
                
                // Store the combined model for future use
                tripleStoreManager.putStore(entryId, QueryType.ANALYSIS, analysisModel);
                
                log.info("Loaded and cached new model for order {} with ANALYSIS query type", entryId);
            } catch (Exception e) {
                log.error("SPARQL query failed for order {}: {}", entryId, e.getMessage());
                throw e;
            }
        } else {
            log.debug("Using cached model for order {} with ANALYSIS query type", entryId);
        }

        try {
            Query rdfQuery = QueryFactory.create(queryString);
            executeQuery(rdfQuery, analysisModel, resultRdfFile);
        } catch (Exception e) {
            log.error("SPARQL query execution failed for order {}: {}", entryId, e.getMessage());
            throw e;
        }

        return resultRdfFile;
    }

    @Transactional(rollbackFor = { SQLException.class, IOException.class })
    public File performSparqlQueryExpertData(long entryId, String queryString) throws SQLException, IOException {
        File resultRdfFile = File.createTempFile("json-result-rdf-file", "json");

        // Check if model exists in store manager first
        Model combinedModel = tripleStoreManager.getStore(entryId, QueryType.EXPERT).orElse(null);
        
        if (combinedModel == null) {
            // Load from database if not in store manager
            GithubRepositoryOrderEntityLobs githubRepositoryOrderEntityLobs = entityManager
                    .find(GithubRepositoryOrderEntityLobs.class, entryId);

            if (githubRepositoryOrderEntityLobs == null) {
                throw NotFoundException.githubEntryNotFound(entryId);
            }

            GithubRepositoryOrderEntity githubRepositoryOrderEntity = githubRepositoryOrderEntityLobs.getOrderEntity();

            if (!githubRepositoryOrderEntity.getStatus().equals(GitRepositoryOrderStatus.DONE)) {
                throw BadRequestException.githubToRdfConversionNotDone(entryId);
            }

            try {
                Model baseModel = loadBaseGitRdfModel(githubRepositoryOrderEntityLobs);
                Model expertModel = loadAnalysisRdfModel(entryId, AnalysisType.EXPERT);
                combinedModel = ModelFactory.createUnion(baseModel, expertModel);
                
                // Store the combined model for future use
                tripleStoreManager.putStore(entryId, QueryType.EXPERT, combinedModel);
                
                log.info("Loaded and cached new model for order {} with EXPERT query type", entryId);
            } catch (Exception e) {
                log.error("SPARQL query failed for order {}: {}", entryId, e.getMessage());
                throw e;
            }
        } else {
            log.debug("Using cached model for order {} with EXPERT query type", entryId);
        }

        try {
            Query rdfQuery = QueryFactory.create(queryString);
            executeQuery(rdfQuery, combinedModel, resultRdfFile);
        } catch (Exception e) {
            log.error("SPARQL query execution failed for order {}: {}", entryId, e.getMessage());
            throw e;
        }

        return resultRdfFile;
    }

    @Transactional(rollbackFor = { SQLException.class, IOException.class })
    public File performSparqlQueryAllData(long entryId, String queryString) throws SQLException, IOException {
        File resultRdfFile = File.createTempFile("json-result-rdf-file", "json");

        // Check if model exists in store manager first
        Model combinedModel = tripleStoreManager.getStore(entryId, QueryType.ALL).orElse(null);
        
        if (combinedModel == null) {
            // Load from database if not in store manager
            GithubRepositoryOrderEntityLobs githubRepositoryOrderEntityLobs = entityManager
                    .find(GithubRepositoryOrderEntityLobs.class, entryId);

            if (githubRepositoryOrderEntityLobs == null) {
                throw NotFoundException.githubEntryNotFound(entryId);
            }

            GithubRepositoryOrderEntity githubRepositoryOrderEntity = githubRepositoryOrderEntityLobs.getOrderEntity();

            if (!githubRepositoryOrderEntity.getStatus().equals(GitRepositoryOrderStatus.DONE)) {
                throw BadRequestException.githubToRdfConversionNotDone(entryId);
            }

            try {
                Model baseModel = loadBaseGitRdfModel(githubRepositoryOrderEntityLobs);
                Model ratingsModel = loadAnalysisRdfModel(entryId, AnalysisType.RATING);
                Model statisticsModel = loadAnalysisRdfModel(entryId, AnalysisType.STATISTIC);
                Model expertModel = loadAnalysisRdfModel(entryId, AnalysisType.EXPERT);
                
                combinedModel = ModelFactory.createUnion(baseModel, ratingsModel);
                combinedModel = ModelFactory.createUnion(combinedModel, statisticsModel);
                combinedModel = ModelFactory.createUnion(combinedModel, expertModel);
                
                // Store the combined model for future use
                tripleStoreManager.putStore(entryId, QueryType.ALL, combinedModel);
                
                log.info("Loaded and cached new model for order {} with ALL query type", entryId);
            } catch (Exception e) {
                log.error("SPARQL query failed for order {}: {}", entryId, e.getMessage());
                throw e;
            }
        } else {
            log.debug("Using cached model for order {} with ALL query type", entryId);
        }

        try {
            Query rdfQuery = QueryFactory.create(queryString);
            executeQuery(rdfQuery, combinedModel, resultRdfFile);
        } catch (Exception e) {
            log.error("SPARQL query execution failed for order {}: {}", entryId, e.getMessage());
            throw e;
        }

        return resultRdfFile;
    }


    private Model loadBaseGitRdfModel(GithubRepositoryOrderEntityLobs lobs)
            throws SQLException, IOException {
        Model baseModel = ModelFactory.createDefaultModel();
        File tempRdfFile = null;

        try {
            tempRdfFile = createTempRdfFile(lobs.getRdfFile());
            baseModel.read(tempRdfFile.getAbsolutePath(), TURTLE_FORMAT);
        } finally {
            if (tempRdfFile != null) {
                FileUtils.deleteQuietly(tempRdfFile);
            }
        }

        return baseModel;
    }

    private Model loadAnalysisRdfModel(long entryId, AnalysisType analysisType) throws SQLException, IOException {
        Model analysisModel = ModelFactory.createDefaultModel();
        List<GithubRepositoryOrderAnalysisEntity> analyses = analysisRepository
                .findAllByGithubRepositoryOrderIdAndAnalysisType(entryId, analysisType);

        for (GithubRepositoryOrderAnalysisEntity analysis : analyses) {
            try {
                Model singleAnalysisModel = loadModelFromBlob(analysis.getRdfBlob());
                analysisModel.add(singleAnalysisModel);
            } catch (Exception e) {
                log.warn("Failed to load {} RDF for analysis ID {} (metricId: {})", 
                        analysisType.name().toLowerCase(), analysis.getId(), analysis.getMetricId());
            }
        }

        log.debug("Loaded {} {} analysis entries for repository order {}", 
                analyses.size(), analysisType.name().toLowerCase(), entryId);
        return analysisModel;
    }

    private Model loadModelFromBlob(Blob rdfBlob) throws SQLException, IOException {
        if (rdfBlob == null) {
            return ModelFactory.createDefaultModel();
        }

        Model model = ModelFactory.createDefaultModel();
        try (InputStream inputStream = new BufferedInputStream(rdfBlob.getBinaryStream());
                StringWriter writer = new StringWriter()) {

            byte[] buffer = new byte[BUFFER_SIZE];
            int length;
            while ((length = inputStream.read(buffer)) != -1) {
                writer.write(new String(buffer, 0, length));
            }

            try (StringReader reader = new StringReader(writer.toString())) {
                model.read(reader, null, TURTLE_FORMAT);
            }
        }

        return model;
    }


    private void executeQuery(Query rdfQuery, Model model, File resultFile) throws IOException {
        if (rdfQuery.isAskType()) {
            try (QueryExecution queryExecution = QueryExecutionFactory.create(rdfQuery, model)) {
                boolean askResult = queryExecution.execAsk();
                writeSimpleStringToFile(askResult ? "yes" : "no", resultFile);
            }
        } else {
            try (QueryExecution queryExecution = QueryExecutionFactory.create(rdfQuery, model);
                    OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(resultFile))) {
                ResultSet resultSet = queryExecution.execSelect();
                ResultSetFormatter.outputAsJSON(outputStream, resultSet);
            }
        }
    }

    private File createTempRdfFile(Blob rdfBlob) throws IOException, SQLException {
        File tempRdfFile = File.createTempFile("temp-rdf-file", ".ttl");

        try (InputStream inputStream = new BufferedInputStream(rdfBlob.getBinaryStream());
                OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(tempRdfFile))) {

            byte[] buffer = new byte[BUFFER_SIZE];
            int length;
            while ((length = inputStream.read(buffer)) != -1) {
                outputStream.write(buffer, 0, length);
            }
        }

        return tempRdfFile;
    }


    private void writeSimpleStringToFile(String content, File file) throws IOException {
        try (FileWriter writer = new FileWriter(file)) {
            writer.write(content);
        }
    }
}