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
import java.util.concurrent.ConcurrentHashMap;

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
import de.leipzig.htwk.gitrdf.database.common.entity.GithubRepositoryOrderRatingEntity;
import de.leipzig.htwk.gitrdf.database.common.entity.GithubRepositoryOrderStatisticEntity;
import de.leipzig.htwk.gitrdf.database.common.entity.enums.GitRepositoryOrderStatus;
import de.leipzig.htwk.gitrdf.database.common.entity.lob.GithubRepositoryOrderEntityLobs;
import de.leipzig.htwk.gitrdf.database.common.repository.GithubRepositoryOrderRatingRepository;
import de.leipzig.htwk.gitrdf.database.common.repository.GithubRepositoryOrderStatisticRepository;
import de.leipzig.htwk.gitrdf.sparql.query.api.exception.BadRequestException;
import de.leipzig.htwk.gitrdf.sparql.query.api.exception.NotFoundException;
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
    private final GithubRepositoryOrderRatingRepository ratingRepository;
    private final GithubRepositoryOrderStatisticRepository statisticRepository;

    // Cache for combined models to improve performance
    private final ConcurrentHashMap<String, Model> modelCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Long> cacheTimestamps = new ConcurrentHashMap<>();
    private static final long CACHE_TTL_MS = 300_000; // 5 minutes

    @Transactional(rollbackFor = { SQLException.class, IOException.class })
    public File performSparqlQuery(long entryId, String queryString) throws SQLException, IOException {
        File resultRdfFile = File.createTempFile("json-result-rdf-file", "json");

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
            Model combinedModel = createCombinedTripleStore(entryId, githubRepositoryOrderEntityLobs);
            Query rdfQuery = QueryFactory.create(queryString);
            executeQuery(rdfQuery, combinedModel, resultRdfFile);
        } catch (Exception e) {
            log.error("SPARQL query failed for order {}: {}", entryId, e.getMessage());
            throw e;
        }

        return resultRdfFile;
    }

    /**
     * Creates a combined triple store merging Git RDF, ratings, and statistics
     * data.
     * Efficiently handles duplicate triples using Jena's union capabilities.
     */
    private Model createCombinedTripleStore(long entryId, GithubRepositoryOrderEntityLobs lobs)
            throws SQLException, IOException {

        String cacheKey = "model_" + entryId;

        if (isCacheValid(cacheKey)) {
            return modelCache.get(cacheKey);
        }

        Model baseModel = loadBaseGitRdfModel(lobs);
        Model ratingsModel = loadRatingsRdfModel(entryId);
        Model statisticsModel = loadStatisticsRdfModel(entryId);
        Model combinedModel = createUnionModel(baseModel, ratingsModel, statisticsModel);

        cacheModel(cacheKey, combinedModel);

        log.debug("Combined triple store created: {} statements total", combinedModel.size());
        return combinedModel;
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

    private Model loadRatingsRdfModel(long entryId) throws SQLException, IOException {
        Model ratingsModel = ModelFactory.createDefaultModel();
        List<GithubRepositoryOrderRatingEntity> ratings = ratingRepository
                .findByGithubRepositoryOrderIdWithRdfData(entryId);

        for (GithubRepositoryOrderRatingEntity rating : ratings) {
            try {
                Model ratingModel = loadModelFromBlob(rating.getRdfBlob());
                ratingsModel.add(ratingModel);
            } catch (Exception e) {
                log.warn("Failed to load rating RDF for rating ID {}", rating.getId());
            }
        }

        return ratingsModel;
    }

    private Model loadStatisticsRdfModel(long entryId) throws SQLException, IOException {
        Model statisticsModel = ModelFactory.createDefaultModel();
        List<GithubRepositoryOrderStatisticEntity> statistics = statisticRepository
                .findByGithubRepositoryOrderIdWithRdfData(entryId);

        for (GithubRepositoryOrderStatisticEntity statistic : statistics) {
            try {
                Model statisticModel = loadModelFromBlob(statistic.getRdfBlob());
                statisticsModel.add(statisticModel);
            } catch (Exception e) {
                log.warn("Failed to load statistic RDF for statistic ID {}", statistic.getId());
            }
        }

        return statisticsModel;
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

    /**
     * Creates a union model that efficiently combines multiple models.
     * Duplicate triples are automatically handled by Jena's union implementation.
     */
    private Model createUnionModel(Model baseModel, Model ratingsModel, Model statisticsModel) {
        Model unionModel = ModelFactory.createUnion(baseModel, ratingsModel);
        unionModel = ModelFactory.createUnion(unionModel, statisticsModel);
        return unionModel;
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

    private boolean isCacheValid(String cacheKey) {
        Long timestamp = cacheTimestamps.get(cacheKey);
        return timestamp != null &&
                (System.currentTimeMillis() - timestamp) < CACHE_TTL_MS &&
                modelCache.containsKey(cacheKey);
    }

    private void cacheModel(String cacheKey, Model model) {
        modelCache.put(cacheKey, model);
        cacheTimestamps.put(cacheKey, System.currentTimeMillis());

        // Simple cache cleanup - remove expired entries
        cleanupExpiredCache();
    }

    private void cleanupExpiredCache() {
        long currentTime = System.currentTimeMillis();
        cacheTimestamps.entrySet().removeIf(entry -> {
            boolean expired = (currentTime - entry.getValue()) > CACHE_TTL_MS;
            if (expired) {
                modelCache.remove(entry.getKey());
            }
            return expired;
        });
    }

    private void writeSimpleStringToFile(String content, File file) throws IOException {
        try (FileWriter writer = new FileWriter(file)) {
            writer.write(content);
        }
    }
}