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
package com.github.sakserv.storm.scheme;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.List;

public class HumiditySensorJsonScheme implements Scheme {

    private static final long serialVersionUID = -2990121166902741545L;

    ObjectMapper mapper = new ObjectMapper();

    @Override
    public List<Object> deserialize(byte[] bytes) {

        int deviceId = 0;
        int humidity = 0;
        int temperature = 0;
        String dt = null;
        try {

            // Convert to string and read as json tree
            String eventDetails = new String(bytes, "UTF-8");
            JsonNode rootNode = mapper.readTree(eventDetails);

            // Get the deviceId
            JsonNode deviceIdNode = rootNode.path("deviceid");
            deviceId = deviceIdNode.asInt();

            // Get the humidity
            JsonNode humidityNode = rootNode.path("humidity");
            humidity = humidityNode.asInt();

            // Get the temperature
            JsonNode temperatureNode = rootNode.path("temperature");
            temperature = temperatureNode.asInt();

            // Get the dt
            JsonNode dtNode = rootNode.path("dt");
            dt = dtNode.asText();

            return new Values(deviceId, humidity, temperature, dt);

        } catch(IOException e) {
            e.printStackTrace();
        }
        return new Values(deviceId, humidity, temperature, dt);
    }

    @Override
    public Fields getOutputFields() {
        return new Fields("deviceid", "humidity", "temperature", "dt");
    }
}
