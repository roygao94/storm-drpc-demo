/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.drpc.DRPCSpout;
import backtype.storm.drpc.ReturnResults;
import backtype.storm.task.ShellBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import java.util.Map;

/**
 * This topology is a basic example of doing distributed RPC on top of Storm. It implements a function that appends a
 * "!" to any string you send the DRPC function.
 *
 * @see <a href="http://storm.apache.org/documentation/Distributed-RPC.html">Distributed RPC</a>
 */
public class ManualPythonDRPC {
    public static class ExclaimBolt extends ShellBolt implements IRichBolt {
        public ExclaimBolt() {
            super("python", "exclaimbolt.py");
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id", "result"));
        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
            return null;
        }
    }

    public static class ParseBolt extends ShellBolt implements IRichBolt {
        public ParseBolt() {
            super("python", "parse.py");
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id", "result"));
        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
            return null;
        }
    }

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        if (args == null || args.length == 0) {
            LocalDRPC drpc = new LocalDRPC();

            DRPCSpout spout = new DRPCSpout("exclamation", drpc);
            builder.setSpout("drpc", spout);
            builder.setBolt("exclaim", new ExclaimBolt(), 3).shuffleGrouping("drpc");
            builder.setBolt("parse", new ParseBolt(), 3).shuffleGrouping("exclaim");
            builder.setBolt("return", new ReturnResults(), 3).shuffleGrouping("parse");

            LocalCluster cluster = new LocalCluster();
            Config conf = new Config();
            cluster.submitTopology("exclaim", conf, builder.createTopology());

            System.out.println(drpc.execute("exclamation", "aaa"));
            System.out.println(drpc.execute("exclamation", "bbb"));
        } else {
            builder.setSpout("drpc", new DRPCSpout("exclamation"));
            builder.setBolt("exclaim", new ExclaimBolt(), 3).shuffleGrouping("drpc");
            builder.setBolt("parse", new ParseBolt(), 3).shuffleGrouping("exclaim");
            builder.setBolt("return", new ReturnResults(), 3).shuffleGrouping("parse");
            Config conf = new Config();

            conf.setNumWorkers(3);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        }
    }
}
