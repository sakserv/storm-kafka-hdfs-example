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
package com.github.sakserv.config;

public class ConfigVars {

    // Zookeeper
    public static final String ZOOKEEPER_PORT_KEY = "zookeeper.port";
    public static final String ZOOKEEPER_HOST_KEY = "zookeeper.host";
    public static final String ZOOKEEPER_TEMP_DIR_KEY = "zookeeper.temp.dir";
    public static final String ZOOKEEPER_CONNECTION_STRING_KEY = "zookeeper.connection.string";

    // Storm
    public static final String STORM_ENABLE_DEBUG_KEY = "storm.enable.debug";
    public static final String STORM_NUM_WORKERS_KEY = "storm.num.workers";
    public static final String STORM_TOPOLOGY_NAME_KEY = "storm.topology.name";

    // Storm Kafka Spout
    public static final String STORM_KAFKA_SPOUT_START_OFFSET_KEY = "storm.kafka.spout.start.offset";
    public static final String STORM_KAFKA_SPOUT_NAME_KEY = "storm.kafka.spout.name";
    public static final String STORM_KAFKA_SPOUT_PARALLELISM_KEY = "storm.kafka.spout.parallelism";
    public static final String STORM_KAFKA_SPOUT_SCHEME_CLASS_KEY = "storm.kafka.spout.scheme.class";

    // Storm HDFS Bolt
    public static final String STORM_HDFS_BOLT_FIELD_DELIMITER_KEY = "storm.hdfs.bolt.field.delimiter";
    public static final String STORM_HDFS_BOLT_OUTPUT_LOCATION_KEY = "storm.hdfs.bolt.output.location";
    public static final String STORM_HDFS_BOLT_SYNC_COUNT_KEY = "storm.hdfs.bolt.sync.count";
    public static final String STORM_HDFS_BOLT_PARALLELISM_KEY = "storm.hdfs.bolt.parallelism";
    public static final String STORM_HDFS_BOLT_NAME_KEY = "storm.hdfs.bolt.name";
    public static final String STORM_HDFS_BOLT_ROTATION_POLICY_UNITS_KEY = "storm.hdfs.bolt.rotation.policy.units";
    public static final String STORM_HDFS_BOLT_ROTATION_POLICY_COUNT_KEY = "storm.hdfs.bolt.rotation.policy.count";

    // Kafka Brokers
    public static final String KAFKA_HOSTNAME_KEY = "kafka.hostname";
    public static final String KAFKA_PORT_KEY = "kafka.port";

    // Kafka Impl
    public static final String KAFKA_TOPIC_KEY = "kafka.topic";
    public static final String KAFKA_START_OFFSET_KEY = "kafka.start.offset";

    // Kafka Test
    public static final String KAFKA_TEST_MESSAGE_COUNT_KEY = "kafka.test.message.count";
    public static final String KAFKA_TEST_BROKER_ID_KEY = "kafka.test.broker.id";
    public static final String KAFKA_TEST_TEMP_DIR_KEY = "kafka.test.temp.dir";

    // HBase
    public static final String HBASE_MASTER_PORT_KEY = "hbase.master.port";
    public static final String HBASE_MASTER_INFO_PORT_KEY = "hbase.master.info.port";
    public static final String HBASE_NUM_REGION_SERVERS_KEY = "hbase.num.region.servers";
    public static final String HBASE_ROOT_DIR_KEY = "hbase.root.dir";
    public static final String HBASE_ZNODE_PARENT_KEY = "hbase.znode.parent";
    public static final String HBASE_WAL_REPLICATION_ENABLED_KEY = "hbase.wal.replication.enabled";

    // HBase Test
    public static final String HBASE_TEST_TABLE_NAME_KEY = "hbase.test.table.name";
    public static final String HBASE_TEST_COL_FAMILY_NAME_KEY = "hbase.test.col.family.name";
    public static final String HBASE_TEST_COL_QUALIFIER_NAME_KEY = "hbase.test.col.qualifier.name";
    public static final String HBASE_TEST_NUM_ROWS_TO_PUT_KEY = "hbase.test.num.rows.to.put";

    //HDFS
    public static final String HDFS_NAMENODE_PORT_KEY = "hdfs.namenode.port";
    public static final String HDFS_TEMP_DIR_KEY = "hdfs.temp.dir";
    public static final String HDFS_NUM_DATANODES_KEY = "hdfs.num.datanodes";
    public static final String HDFS_ENABLE_PERMISSIONS_KEY = "hdfs.enable.permissions";
    public static final String HDFS_FORMAT_KEY = "hdfs.format";
    public static final String HDFS_ENABLE_RUNNING_USER_AS_PROXY_USER = "hdfs.enable.running.user.as.proxy.user";
    public static final String HDFS_DEFAULT_FS_KEY = "hdfs.default.fs";

    // HDFS Test
    public static final String HDFS_TEST_FILE_KEY = "hdfs.test.file";
    public static final String HDFS_TEST_STRING_KEY = "hdfs.test.string";

}
