package com.crusbee.hazel;

import com.crusbee.model.dto.DrillExtractionDto;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.Util;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.*;

public class HazelApacheDrillSourceImpl {

    private static final String JDBC_DRIVER = "org.apache.drill.jdbc.Driver";
    private static String CONNECTION;
    private static final String QUERY_CATEGORIES = "" +
            "SELECT t.cCat.externalId,t.cCat.name.textValue, t.cCat.description.textValue " +
            "FROM (SELECT flatten(childCategories) AS cCat FROM dfs.`%s`) t";
    private static final String QUERY_PRODUCTS = "" +
            "SELECT p.product.externalId, p.product.name.textValue, p.product.description.textValue " +
            "FROM (SELECT flatten(t.flatdata.products) as product " +
            "FROM (SELECT flatten(childCategories) AS flatdata FROM dfs.`%s`) t) p";
    private static final String QUERY_PRODUCTS_SKU = "" +
            "SELECT s.sku.defaultProduct, s.sku.name.textValue,s.sku.description.textValue " +
            "FROM (SELECT flatten(p.product.additionalScus) as sku " +
            "FROM (SELECT flatten(t.flatdata.products) as product " +
            "FROM (SELECT flatten(childCategories) AS flatdata FROM dfs.`%s`) t) p) s";
    private static final String QUERY_PRODUCTS_PARTS = "" +
            "SELECT parts.part.externalId, parts.part.name.textValue,parts.part.description.textValue " +
            "FROM (SELECT flatten(p.product.parts) as part " +
            "FROM (SELECT flatten(t.flatdata.products) as product " +
            "FROM (SELECT flatten(childCategories) AS flatdata FROM dfs.`%s`) t) p) parts";
    private static final Map<String, String> queriesMap = new HashMap<>();


    static {
        init();
    }
    private JetInstance jet;
    private String dbDirectory;

    public HazelApacheDrillSourceImpl(JetInstance jet, String dbDirectory) {
        this.jet = jet;
        this.dbDirectory = dbDirectory;
    }

    private static void init() {
        System.setProperty("hazelcast.logging.type", "log4j");
        try (InputStream input = HazelApacheDrillSourceImpl.class.getClassLoader().getResourceAsStream("jdbc.properties")) {
            Properties prop = new Properties();
            if (input == null) {
                System.out.println("Sorry, unable to find config.properties");
                return;
            }
            prop.load(input);
            String file = prop.getProperty("jdbc.query.filepath");
            CONNECTION = prop.getProperty("jdbc.connection");
            queriesMap.put("categories", String.format(QUERY_CATEGORIES, file));
            queriesMap.put("products", String.format(QUERY_PRODUCTS, file));
            queriesMap.put("productSku", String.format(QUERY_PRODUCTS_SKU, file));
            queriesMap.put("productPars", String.format(QUERY_PRODUCTS_PARTS, file));
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    private static Pipeline buildPipeline(String connectionUrl, Map.Entry<String, String> query) {
        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.jdbc(connectionUrl,
                query.getValue(),
                resultSet -> new DrillExtractionDto(resultSet.getString(1), resultSet.getString(2), resultSet.getString(3))))
                .map(dto -> {
                    System.out.println("------------- " + dto.getExternalId());
                    return Util.entry(dto.getExternalId(), dto);
                })
                .drainTo(Sinks.map(query.getKey()));
        return p;
    }

    public JetInstance getJet() {
        return jet;
    }

    public static void main(String[] args) throws IOException {
        HazelApacheDrillSourceImpl source = new HazelApacheDrillSourceImpl(Jet.newJetInstance(), Files.createTempDirectory(HazelApacheDrillSourceImpl.class.getName()).toString());
        try {
            Class.forName(JDBC_DRIVER);
            List<Job> jobs = new ArrayList<>();
            queriesMap.entrySet().forEach(entry -> {
                try {
                    jobs.add(source.getJet().newJob(source.createPipeLine(entry)));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
            jobs.forEach(Job::join);
            queriesMap.entrySet().forEach(entry -> source.getJet().getMap(entry.getKey()).values().forEach(System.out::println));
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            source.cleanup();
        }
    }

    private Pipeline createPipeLine(Map.Entry<String, String> query) throws Exception {
        Pipeline p = buildPipeline(connectionUrl(), query);
        return p;
    }
    private void cleanup() {
        Jet.shutdownAll();
    }
    private String connectionUrl() {
        return CONNECTION;
    }
}