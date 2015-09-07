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

import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.TimedRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsBoltConfigBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(HdfsBoltConfigBuilder.class);

    private String fieldDelimiter;
    private String outputLocation;
    private String hdfsDefaultFs;
    private Integer syncCount;
    private Integer boltParallelism;
    private String boltName;
    private FileRotationPolicy fileRotationPolicy;

    public String getFieldDelimiter() {
        return fieldDelimiter;
    }

    public String getOutputLocation() {
        return outputLocation;
    }

    public String getHdfsDefaultFs() {
        return hdfsDefaultFs;
    }

    public Integer getSyncCount() {
        return syncCount;
    }

    public Integer getBoltParallelism() {
        return boltParallelism;
    }

    public String getBoltName() {
        return boltName;
    }

    public FileRotationPolicy getFileRotationPolicy() {
        return fileRotationPolicy;
    }

    private HdfsBoltConfigBuilder(Builder builder) {
        this.fieldDelimiter = builder.fieldDelimiter;
        this.outputLocation = builder.outputLocation;
        this.hdfsDefaultFs = builder.hdfsDefaultFs;
        this.syncCount = builder.syncCount;
        this.boltParallelism = builder.boltParallelism;
        this.boltName = builder.boltName;
        this.fileRotationPolicy = builder.fileRotationPolicy;
    }

    public static class Builder {
        private String fieldDelimiter;
        private String outputLocation;
        private String hdfsDefaultFs;
        private Integer syncCount;
        private Integer boltParallelism;
        private String boltName;
        private FileRotationPolicy fileRotationPolicy;

        public Builder setFieldDelimiter(String fieldDelimiter) {
            this.fieldDelimiter = fieldDelimiter;
            return this;
        }

        public Builder setOutputLocation(String outputLocation) {
            this.outputLocation = outputLocation;
            return this;
        }

        public Builder setHdfsDefaultFs(String hdfsDefaultFs) {
            this.hdfsDefaultFs = hdfsDefaultFs;
            return this;
        }

        public Builder setSyncCount(Integer syncCount) {
            this.syncCount = syncCount;
            return this;
        }

        public Builder setBoltParallelism(Integer boltParallelism) {
            this.boltParallelism = boltParallelism;
            return this;
        }


        public Builder setBoltName(String boltName) {
            this.boltName = boltName;
            return this;
        }

        public Builder setFileRotationPolicy(FileRotationPolicy fileRotationPolicy) {
            this.fileRotationPolicy = fileRotationPolicy;
            return this;
        }


        public HdfsBoltConfigBuilder build() {
            HdfsBoltConfigBuilder hdfsBoltConfigBuilder = new HdfsBoltConfigBuilder(this);
            validateObject(hdfsBoltConfigBuilder);
            return hdfsBoltConfigBuilder;
        }


        private void validateObject(HdfsBoltConfigBuilder hdfsBoltConfigBuilder) {

            if (hdfsBoltConfigBuilder.getFieldDelimiter() == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: HDFS Bolt Field Delimiter");
            }

            if (hdfsBoltConfigBuilder.getOutputLocation() == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: HDFS Bolt Output Location");
            }

            if (hdfsBoltConfigBuilder.getHdfsDefaultFs() == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: HDFS Default Fs");
            }

            if (hdfsBoltConfigBuilder.getSyncCount() == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: HDFS Bolt Sync Count");
            }

            if (hdfsBoltConfigBuilder.getBoltParallelism() == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: HDFS Bolt Parallelism");
            }

            if (hdfsBoltConfigBuilder.getFileRotationPolicy() == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: HDFS Bolt File Rotation Policy");
            }

            if (hdfsBoltConfigBuilder.getBoltName() == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: HDFS Bolt Name");
            }
        }
    }

    public HdfsBolt getHdfsBolt() {

        LOG.info("HDFSBOLT: Configuring the HdfsBolt");

        // Define the RecordFormat, SyncPolicy, and FileNameFormat
        RecordFormat format = new DelimitedRecordFormat().withFieldDelimiter(fieldDelimiter);
        SyncPolicy syncPolicy = new CountSyncPolicy(syncCount);
        FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath(outputLocation);

        // Configure the Bolt
        return new HdfsBolt()
                .withFsUrl(hdfsDefaultFs)
                .withFileNameFormat(fileNameFormat)
                .withRecordFormat(format)
                .withRotationPolicy(fileRotationPolicy)
                .withSyncPolicy(syncPolicy);

    }

    public static FileRotationPolicy getTimeBasedFileRotationPolicy(String rotationUnits, int rotationCount) {
        TimedRotationPolicy.TimeUnit units;
        if (rotationUnits.toUpperCase().equals("SECONDS")) {
            units = TimedRotationPolicy.TimeUnit.SECONDS;
        } else if (rotationUnits.toUpperCase().equals("MINUTES")) {
            units = TimedRotationPolicy.TimeUnit.MINUTES;
        } else if (rotationUnits.toUpperCase().equals("HOURS")) {
            units = TimedRotationPolicy.TimeUnit.HOURS;
        } else if (rotationUnits.toUpperCase().equals("DAYS")) {
            units = TimedRotationPolicy.TimeUnit.DAYS;
        } else {
            units = TimedRotationPolicy.TimeUnit.MINUTES;
        }

        return new TimedRotationPolicy(rotationCount, units);
    }

}
