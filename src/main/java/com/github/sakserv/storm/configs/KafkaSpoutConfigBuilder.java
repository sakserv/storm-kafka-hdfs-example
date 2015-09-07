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
package com.github.sakserv.storm.configs;

import backtype.storm.spout.Scheme;
import backtype.storm.spout.SchemeAsMultiScheme;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

import java.util.UUID;

public class KafkaSpoutConfigBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaSpoutConfigBuilder.class);

    private String zookeeperConnectionString;
    private String kafkaTopic;
    private String kafkaStartOffset;
    private Integer spoutParallelismHint;
    private String spoutName;
    private String spoutSchemeClass;

    public String getZookeeperConnectionString() {
        return zookeeperConnectionString;
    }

    public String getKafkaTopic() {
        return kafkaTopic;
    }

    public String getKafkaStartOffset() {
        return kafkaStartOffset;
    }

    public Integer getSpoutParallelismHint() {
        return spoutParallelismHint;
    }

    public String getSpoutName() {
        return spoutName;
    }

    public String getSpoutSchemeClass() {
        return spoutSchemeClass;
    }

    private KafkaSpoutConfigBuilder(Builder builder) {
        this.zookeeperConnectionString = builder.zookeeperConnectionString;
        this.kafkaTopic = builder.kafkaTopic;
        this.kafkaStartOffset = builder.kafkaStartOffset;
        this.spoutParallelismHint = builder.spoutParallelismHint;
        this.spoutName = builder.spoutName;
        this.spoutSchemeClass = builder.spoutSchemeClass;
    }

    public static class Builder {
        private String zookeeperConnectionString;
        private String kafkaTopic;
        private String kafkaStartOffset;
        private Integer spoutParallelismHint;
        private String spoutName;
        private String spoutSchemeClass;

        public Builder setZookeeperConnectionString(String zookeeperConnectionString) {
            this.zookeeperConnectionString = zookeeperConnectionString;
            return this;
        }

        public Builder setKafkaTopic(String kafkaTopic) {
            this.kafkaTopic = kafkaTopic;
            return this;
        }

        public Builder setKafkaStartOffset(String kafkaStartOffset) {
            this.kafkaStartOffset = kafkaStartOffset;
            return this;
        }

        public Builder setSpoutParallelismHint(Integer spoutParallelismHint) {
            this.spoutParallelismHint = spoutParallelismHint;
            return this;
        }

        public Builder setSpoutName(String spoutName) {
            this.spoutName = spoutName;
            return this;
        }

        public Builder setSpoutSchemeClass(String spoutSchemeClass) {
            this.spoutSchemeClass = spoutSchemeClass;
            return this;
        }


        public KafkaSpoutConfigBuilder build() {
            KafkaSpoutConfigBuilder configureKafkaSpout = new KafkaSpoutConfigBuilder(this);
            validateObject(configureKafkaSpout);
            return configureKafkaSpout;
        }


        private void validateObject(KafkaSpoutConfigBuilder kafkaSpoutConfigBuilder) {

            if (kafkaSpoutConfigBuilder.getZookeeperConnectionString() == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: Zookeeper Connection String");
            }

            if (kafkaSpoutConfigBuilder.getKafkaTopic() == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: Kafka Topic");
            }

            if (kafkaSpoutConfigBuilder.getKafkaStartOffset() == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: Kafka Start Offset");
            }

            if (kafkaSpoutConfigBuilder.getSpoutName() == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: Spout Name");
            }

            if (kafkaSpoutConfigBuilder.getSpoutParallelismHint() == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: Spout Parallelism Hint");
            }

            if (kafkaSpoutConfigBuilder.getSpoutSchemeClass() == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: Spout Scheme Class");
            }
        }
    }

    public KafkaSpout getKafkaSpout() {

        LOG.info("KAFKASPOUT: Configuring the Kafka Spout");

        // Create the initial spoutConfig
        SpoutConfig spoutConfig = new SpoutConfig(new ZkHosts(zookeeperConnectionString),
                kafkaTopic,      // Kafka topic to read from
                "/" + kafkaTopic, // Root path in Zookeeper for the spout to store consumer offsets
                UUID.randomUUID().toString());  // ID for storing consumer offsets in Zookeeper

        // Set the scheme
        try {
            spoutConfig.scheme = new SchemeAsMultiScheme(getSchemeFromClassName(spoutSchemeClass));
        } catch(Exception e) {
            LOG.error("ERROR: Unable to create instance of scheme: " + spoutSchemeClass);
            e.printStackTrace();
        }

        // Set the offset
        setKafkaOffset(spoutConfig, kafkaStartOffset);

        // Create the kafkaSpout
        return new KafkaSpout(spoutConfig);

    }

    private static void setKafkaOffset(SpoutConfig spoutConfig, String kafkaStartOffset) {
        // Allow for passing in an offset time
        // startOffsetTime has a bug that ignores the special -2 value
        if(kafkaStartOffset.equals("-2")) {
            spoutConfig.startOffsetTime = kafka.api.OffsetRequest.EarliestTime();
        } else if (kafkaStartOffset != null) {
            spoutConfig.startOffsetTime = Long.parseLong(kafkaStartOffset);
        }

    }

    private static Scheme getSchemeFromClassName(String spoutSchemeCls) throws Exception {
        return (Scheme)Class.forName(spoutSchemeCls).getConstructor().newInstance();
    }

}
