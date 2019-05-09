package com.crusbee.hazel;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Util;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;

import java.nio.file.Files;

public class JdbcSource {

    private static final String MAP_NAME = "userMap";
    private static final String TABLE_NAME = "cp.`employee.json`";
    private static final String JDBC_DRIVER = "org.apache.drill.jdbc.Driver";

    private JetInstance jet;
    private String dbDirectory;

    private static Pipeline buildPipeline(String connectionUrl) throws ClassNotFoundException {
        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.jdbc(connectionUrl,
                "SELECT * FROM " + TABLE_NAME + " LIMIT 10",
                resultSet -> new User(resultSet.getString(1), resultSet.getString(2))))
                .map(user -> {
                    System.out.println("------------- " + user.getId());
                    return Util.entry(user.getId(), user);})
                .drainTo(Sinks.map(MAP_NAME));
        return p;
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("hazelcast.logging.type", "log4j");
        Class.forName(JDBC_DRIVER);
        new JdbcSource().go();
    }

    private void go() throws Exception {
        try {
            setup();
            Pipeline p = buildPipeline(connectionUrl());
            jet.newJob(p).join();
            jet.getMap(MAP_NAME).values().forEach(System.out::println);
        } finally {
            cleanup();
        }
    }

    private void setup() throws Exception {
        dbDirectory = Files.createTempDirectory(JdbcSource.class.getName()).toString();
        jet = Jet.newJetInstance();
    }

    private void cleanup() {
        Jet.shutdownAll();
    }

    private String connectionUrl() {
        return "jdbc:drill:drillbit=localhost";
    }
}