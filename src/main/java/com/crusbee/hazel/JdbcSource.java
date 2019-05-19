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

public class JdbcSource {


    private static final String QUERY_CATEGORIES = "Select t.cCat.externalId,t.cCat.name.textValue, t.cCat.description.textValue from (select flatten(childCategories) AS cCat FROM dfs.`%s`) t";
    private static final String QUERY_PRODUCTS = "Select p.product.externalId, p.product.name.textValue, p.product.description.textValue from (SELECT flatten(t.flatdata.products) as product FROM (select flatten(childCategories) AS flatdata FROM dfs.`%s`) t) p";
    private static final String QUERY_PRODUCTS_SKU = "Select p.product.externalId, p.product.name.textValue from (SELECT flatten(t.flatdata.products) as product FROM (select flatten(childCategories) AS flatdata FROM dfs.`C:\\Users\\Stanislav\\Downloads\\apache_drill\\src\\main\\resources\\catalognew.json`) t) p";
    private static final String QUERY_PRODUCTS_PARTS = "Select p.product.externalId, p.product.name.textValue from (SELECT flatten(t.flatdata.products) as product FROM (select flatten(childCategories) AS flatdata FROM dfs.`C:\\Users\\Stanislav\\Downloads\\apache_drill\\src\\main\\resources\\catalognew.json`) t) p";
    private static final Map<String, String> queriesMap = new HashMap<>();
    static {
        init();
    }

    private static final String JDBC_DRIVER = "org.apache.drill.jdbc.Driver";
    private JetInstance jet;
    private String dbDirectory;

    public JdbcSource(JetInstance jet, String dbDirectory) {
        this.jet = jet;
        this.dbDirectory = dbDirectory;
    }

    private static void init() {
        try (InputStream input = JdbcSource.class.getClassLoader().getResourceAsStream("query.properties")) {
            Properties prop = new Properties();
            if (input == null) {
                System.out.println("Sorry, unable to find config.properties");
                return;
            }
            prop.load(input);
            String file = prop.getProperty("query.filepath");
            queriesMap.put("categories", String.format(QUERY_CATEGORIES, file));
            queriesMap.put("products", String.format(QUERY_PRODUCTS, file));
//        queriesMap.put("productSku", QUERY_PRODUCTS_SKU);
//        queriesMap.put("productPars", QUERY_PRODUCTS_PARTS);

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

    public static void main(String[] args) throws Exception {
        JdbcSource source = new JdbcSource(Jet.newJetInstance(), Files.createTempDirectory(JdbcSource.class.getName()).toString());
        try {
            System.setProperty("hazelcast.logging.type", "log4j");
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
        return "jdbc:drill:drillbit=localhost";
    }
}