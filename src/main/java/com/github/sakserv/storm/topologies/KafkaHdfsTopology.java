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
package com.github.sakserv.storm.topologies;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import com.github.sakserv.config.ConfigVars;
import com.github.sakserv.propertyparser.PropertyParser;
import com.github.sakserv.storm.configs.HdfsBoltConfigBuilder;
import com.github.sakserv.storm.configs.KafkaSpoutConfigBuilder;
import com.github.sakserv.storm.configs.StormConfig;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;

public class KafkaHdfsTopology {

    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            System.out.println("USAGE: storm jar </path/to/topo.jar> <com.package.TopologyMainClass> " +
                    "</path/to/config.properties>");
            System.exit(1);
        }
        String propFilePath = args[0];

        // Setup the property parser
        PropertyParser propertyParser = new PropertyParser(propFilePath);
        propertyParser.parsePropsFile();

        // Create the topology builder object
        TopologyBuilder topologyBuilder = new TopologyBuilder();

        // Setup the Kafka Spout
        KafkaSpoutConfigBuilder kafkaSpoutConfigBuilder = new KafkaSpoutConfigBuilder.Builder()
                .setZookeeperConnectionString(propertyParser.getProperty(ConfigVars.ZOOKEEPER_CONNECTION_STRING_KEY))
                .setKafkaTopic(propertyParser.getProperty(ConfigVars.KAFKA_TOPIC_KEY))
                .setKafkaStartOffset(propertyParser.getProperty(ConfigVars.KAFKA_START_OFFSET_KEY))
                .setSpoutName(propertyParser.getProperty(ConfigVars.STORM_KAFKA_SPOUT_NAME_KEY))
                .setSpoutParallelismHint(Integer.parseInt(
                        propertyParser.getProperty(ConfigVars.STORM_KAFKA_SPOUT_PARALLELISM_KEY)))
                .setSpoutSchemeClass(propertyParser.getProperty(ConfigVars.STORM_KAFKA_SPOUT_SCHEME_CLASS_KEY))
                .build();


        // Add the spout to the topology
        topologyBuilder.setSpout(kafkaSpoutConfigBuilder.getSpoutName(), kafkaSpoutConfigBuilder.getKafkaSpout(),
                kafkaSpoutConfigBuilder.getSpoutParallelismHint());


        // Setup the HDFS Bolt
        FileRotationPolicy fileRotationPolicy = HdfsBoltConfigBuilder.getTimeBasedFileRotationPolicy(
                propertyParser.getProperty(ConfigVars.STORM_HDFS_BOLT_ROTATION_POLICY_UNITS_KEY),
                Integer.parseInt(propertyParser.getProperty(ConfigVars.STORM_HDFS_BOLT_ROTATION_POLICY_COUNT_KEY)));
        HdfsBoltConfigBuilder hdfsBoltConfigBuilder = new HdfsBoltConfigBuilder.Builder()
                .setFieldDelimiter(propertyParser.getProperty(ConfigVars.STORM_HDFS_BOLT_FIELD_DELIMITER_KEY))
                .setOutputLocation(propertyParser.getProperty(ConfigVars.STORM_HDFS_BOLT_OUTPUT_LOCATION_KEY))
                .setHdfsDefaultFs(propertyParser.getProperty(ConfigVars.HDFS_DEFAULT_FS_KEY))
                .setSyncCount(Integer.parseInt(propertyParser.getProperty(ConfigVars.STORM_HDFS_BOLT_SYNC_COUNT_KEY)))
                .setBoltParallelism(Integer.parseInt(
                        propertyParser.getProperty(ConfigVars.STORM_HDFS_BOLT_PARALLELISM_KEY)))
                .setBoltName(propertyParser.getProperty(ConfigVars.STORM_HDFS_BOLT_NAME_KEY))
                .setFileRotationPolicy(fileRotationPolicy)
                .build();


        // Add the HDFS Bolt to the topology
        topologyBuilder.setBolt(hdfsBoltConfigBuilder.getBoltName(), hdfsBoltConfigBuilder.getHdfsBolt(),
                hdfsBoltConfigBuilder.getBoltParallelism()).shuffleGrouping(kafkaSpoutConfigBuilder.getSpoutName());


        // Storm Topology Config
        Config stormConfig = StormConfig.createStormConfig(
                Boolean.parseBoolean(propertyParser.getProperty(ConfigVars.STORM_ENABLE_DEBUG_KEY)),
                Integer.parseInt(propertyParser.getProperty(ConfigVars.STORM_NUM_WORKERS_KEY)));

        // Submit the topology
        StormSubmitter.submitTopologyWithProgressBar(propertyParser.getProperty(ConfigVars.STORM_TOPOLOGY_NAME_KEY),
                stormConfig, topologyBuilder.createTopology());

    }
}
