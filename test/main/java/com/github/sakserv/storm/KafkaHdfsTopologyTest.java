/*
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.github.sakserv.storm;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import com.github.sakserv.config.ConfigVars;
import com.github.sakserv.kafka.producer.KafkaReadfileProducer;
import com.github.sakserv.minicluster.impl.*;
import com.github.sakserv.propertyparser.PropertyParser;
import com.github.sakserv.storm.configs.HdfsBoltConfigBuilder;
import com.github.sakserv.storm.configs.KafkaSpoutConfigBuilder;
import com.github.sakserv.storm.configs.StormConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class KafkaHdfsTopologyTest {

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(KafkaHdfsTopologyTest.class);

    // Kafka input file
    private static final String kafkaProducerInputFile = "test/main/resources/test_input.txt";

    // Setup the property parser
    private static final String testPropsFile = "test.properties";
    private static PropertyParser propertyParser;
    static {
        try {
            propertyParser = new PropertyParser(testPropsFile);
            propertyParser.parsePropsFile();
        } catch(IOException e) {
            LOG.error("Unable to load property file: " + testPropsFile);
        }
    }

    private static ZookeeperLocalCluster zookeeperLocalCluster;
    private static StormLocalCluster stormLocalCluster;
    private static KafkaLocalBroker kafkaLocalBroker;
    private static HdfsLocalCluster hdfsLocalCluster;

    @BeforeClass
    public static void setUp() throws Exception {

        // Start the zookeeper local cluster
        zookeeperLocalCluster = new ZookeeperLocalCluster.Builder()
                .setPort(Integer.parseInt(propertyParser.getProperty(ConfigVars.ZOOKEEPER_PORT_KEY)))
                .setTempDir(propertyParser.getProperty(ConfigVars.ZOOKEEPER_TEMP_DIR_KEY))
                .setZookeeperConnectionString(propertyParser.getProperty(ConfigVars.ZOOKEEPER_CONNECTION_STRING_KEY))
                .build();
        zookeeperLocalCluster.start();

        // Start the Storm local cluster
        stormLocalCluster = new StormLocalCluster.Builder()
                .setZookeeperHost(propertyParser.getProperty(ConfigVars.ZOOKEEPER_HOST_KEY))
                .setZookeeperPort(Long.parseLong(propertyParser.getProperty(ConfigVars.ZOOKEEPER_PORT_KEY)))
                .setEnableDebug(Boolean.parseBoolean(propertyParser.getProperty(ConfigVars.STORM_ENABLE_DEBUG_KEY)))
                .setNumWorkers(Integer.parseInt(propertyParser.getProperty(ConfigVars.STORM_NUM_WORKERS_KEY)))
                .setStormConfig(new Config())
                .build();
        stormLocalCluster.start();

        // Start the kafka local broker
        kafkaLocalBroker = new KafkaLocalBroker.Builder()
                .setKafkaHostname(propertyParser.getProperty(ConfigVars.KAFKA_HOSTNAME_KEY))
                .setKafkaPort(Integer.parseInt(propertyParser.getProperty(ConfigVars.KAFKA_PORT_KEY)))
                .setKafkaBrokerId(Integer.parseInt(propertyParser.getProperty(ConfigVars.KAFKA_TEST_BROKER_ID_KEY)))
                .setKafkaProperties(new Properties())
                .setKafkaTempDir(propertyParser.getProperty(ConfigVars.KAFKA_TEST_TEMP_DIR_KEY))
                .setZookeeperConnectionString(propertyParser.getProperty(ConfigVars.ZOOKEEPER_CONNECTION_STRING_KEY))
                .build();
        kafkaLocalBroker.start();

        // Start the HDFS local cluster
        hdfsLocalCluster = new HdfsLocalCluster.Builder()
                .setHdfsNamenodePort(Integer.parseInt(propertyParser.getProperty(ConfigVars.HDFS_NAMENODE_PORT_KEY)))
                .setHdfsTempDir(propertyParser.getProperty(ConfigVars.HDFS_TEMP_DIR_KEY))
                .setHdfsNumDatanodes(Integer.parseInt(propertyParser.getProperty(ConfigVars.HDFS_NUM_DATANODES_KEY)))
                .setHdfsEnablePermissions(
                        Boolean.parseBoolean(propertyParser.getProperty(ConfigVars.HDFS_ENABLE_PERMISSIONS_KEY)))
                .setHdfsFormat(Boolean.parseBoolean(propertyParser.getProperty(ConfigVars.HDFS_FORMAT_KEY)))
                .setHdfsEnableRunningUserAsProxyUser(Boolean.parseBoolean(
                        propertyParser.getProperty(ConfigVars.HDFS_ENABLE_RUNNING_USER_AS_PROXY_USER)))
                .setHdfsConfig(new Configuration())
                .build();
        hdfsLocalCluster.start();

    }

    @AfterClass
    public static void tearDown() throws Exception {

        // Stop storm, wait 10 seconds to left the final flush to hdfs finish
        stormLocalCluster.stop(propertyParser.getProperty(ConfigVars.STORM_TOPOLOGY_NAME_KEY));
        Thread.sleep(5000);

        // Stop kafka, hdfs, and zookeeper
        kafkaLocalBroker.stop();
        hdfsLocalCluster.stop();
        zookeeperLocalCluster.stop();
    }

    @Test
    public void testKafkaHbaseHdfsTopology() throws Exception {

        // Put messages in Kafka
        produceMessageToKafka();

        // Create the topology builder object
        TopologyBuilder topologyBuilder = new TopologyBuilder();

        // Setup the Kafka Spout and add it to the topology
        KafkaSpoutConfigBuilder kafkaSpoutConfigBuilder = getKafkaSpoutConfigBuilder();
        topologyBuilder.setSpout(kafkaSpoutConfigBuilder.getSpoutName(), kafkaSpoutConfigBuilder.getKafkaSpout(),
                kafkaSpoutConfigBuilder.getSpoutParallelismHint());


        // Setup the HDFS Bolt and add it to the topology
        HdfsBoltConfigBuilder hdfsBoltConfigBuilder = getHdfsBoltConfigBuilder();
        topologyBuilder.setBolt(hdfsBoltConfigBuilder.getBoltName(), hdfsBoltConfigBuilder.getHdfsBolt(),
                hdfsBoltConfigBuilder.getBoltParallelism()).shuffleGrouping(kafkaSpoutConfigBuilder.getSpoutName());


        // Setup the Storm configuration
        Config stormConfig = StormConfig.createStormConfig(
                Boolean.parseBoolean(propertyParser.getProperty(ConfigVars.STORM_ENABLE_DEBUG_KEY)),
                Integer.parseInt(propertyParser.getProperty(ConfigVars.STORM_NUM_WORKERS_KEY)));

        //stormConfig.putAll(hbaseLocalCluster.getHbaseConfiguration());

        // Submit the topology
        stormLocalCluster.submitTopology(propertyParser.getProperty(ConfigVars.STORM_TOPOLOGY_NAME_KEY),
                stormConfig, topologyBuilder.createTopology());

        // Let the topology run
        Thread.sleep(30000);

        // Validate the results match the input
        validateHdfsResults();


    }

    /**
     * Validate that the files in HDFS contain the expected data from Kafka
     * @throws Exception
     */
    private void validateHdfsResults() throws Exception {
        LOG.info("HDFS: VALIDATING");

        // Get the filesystem handle and a list of files written by the test
        FileSystem hdfsFsHandle = hdfsLocalCluster.getHdfsFileSystemHandle();
        RemoteIterator<LocatedFileStatus> listFiles = hdfsFsHandle.listFiles(
                new Path(propertyParser.getProperty(ConfigVars.STORM_HDFS_BOLT_OUTPUT_LOCATION_KEY)), true);

        // Loop through the files and count up the lines
        int count = 0;
        while (listFiles.hasNext()) {
            LocatedFileStatus file = listFiles.next();

            LOG.info("HDFS READ: Found File: " + file);

            BufferedReader br = new BufferedReader(new InputStreamReader(hdfsFsHandle.open(file.getPath())));
            String line = br.readLine();
            while (line != null) {
                LOG.info("HDFS READ: Found Line: " + line);
                line = br.readLine();
                count++;
            }
        }
        hdfsFsHandle.close();

        // Validate the number of lines matches the number of kafka messages
        assertEquals(Integer.parseInt(propertyParser.getProperty(ConfigVars.KAFKA_TEST_MESSAGE_COUNT_KEY)), count);
    }

    /**
     * Creates a KafkaReadfileProducer and sends the input files contents to Kafka
     */
    private void produceMessageToKafka() {
        KafkaReadfileProducer kafkaReadfileProducer = new KafkaReadfileProducer.Builder()
                .setKafkaHostname(propertyParser.getProperty(ConfigVars.KAFKA_HOSTNAME_KEY))
                .setKafkaPort(Integer.parseInt(propertyParser.getProperty(ConfigVars.KAFKA_PORT_KEY)))
                .setTopic(propertyParser.getProperty(ConfigVars.KAFKA_TOPIC_KEY))
                .setInputFileName(kafkaProducerInputFile)
                .build();

        kafkaReadfileProducer.produceMessages();

    }

    /**
     * Returns the KafkaSpoutConfigBuilder ready for use
     * @return KafkaSpoutConfigBuilder Used to construct the kafka spout configuration
     */
    private KafkaSpoutConfigBuilder getKafkaSpoutConfigBuilder() {
        return new KafkaSpoutConfigBuilder.Builder()
                .setZookeeperConnectionString(propertyParser.getProperty(ConfigVars.ZOOKEEPER_CONNECTION_STRING_KEY))
                .setKafkaTopic(propertyParser.getProperty(ConfigVars.KAFKA_TOPIC_KEY))
                .setKafkaStartOffset(propertyParser.getProperty(ConfigVars.STORM_KAFKA_SPOUT_START_OFFSET_KEY))
                .setSpoutName(propertyParser.getProperty(ConfigVars.STORM_KAFKA_SPOUT_NAME_KEY))
                .setSpoutParallelismHint(Integer.parseInt(
                        propertyParser.getProperty(ConfigVars.STORM_KAFKA_SPOUT_PARALLELISM_KEY)))
                .setSpoutSchemeClass(propertyParser.getProperty(ConfigVars.STORM_KAFKA_SPOUT_SCHEME_CLASS_KEY))
                .build();
    }

    /**
     * Returns the HdfsBoltConfigBuilder ready for use
     * @return HdfsBoltConfigBuilder Used to construct the kafka spout configuration
     */
    private HdfsBoltConfigBuilder getHdfsBoltConfigBuilder() {
        FileRotationPolicy fileRotationPolicy = HdfsBoltConfigBuilder.getTimeBasedFileRotationPolicy(
                propertyParser.getProperty(ConfigVars.STORM_HDFS_BOLT_ROTATION_POLICY_UNITS_KEY),
                Integer.parseInt(propertyParser.getProperty(ConfigVars.STORM_HDFS_BOLT_ROTATION_POLICY_COUNT_KEY)));

        return new HdfsBoltConfigBuilder.Builder()
                .setFieldDelimiter(propertyParser.getProperty(ConfigVars.STORM_HDFS_BOLT_FIELD_DELIMITER_KEY))
                .setOutputLocation(propertyParser.getProperty(ConfigVars.STORM_HDFS_BOLT_OUTPUT_LOCATION_KEY))
                .setHdfsDefaultFs(propertyParser.getProperty(ConfigVars.HDFS_DEFAULT_FS_KEY))
                .setSyncCount(Integer.parseInt(propertyParser.getProperty(ConfigVars.STORM_HDFS_BOLT_SYNC_COUNT_KEY)))
                .setBoltParallelism(Integer.parseInt(
                        propertyParser.getProperty(ConfigVars.STORM_HDFS_BOLT_PARALLELISM_KEY)))
                .setBoltName(propertyParser.getProperty(ConfigVars.STORM_HDFS_BOLT_NAME_KEY))
                .setFileRotationPolicy(fileRotationPolicy)
                .build();
    }

}
