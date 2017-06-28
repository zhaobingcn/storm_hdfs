package com.learn.storm_hdfs;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.TimedRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Random;

/**
 * Hello world!
 *
 */
public class StormToHDFSTopology
{

    public static class EventSpout extends BaseRichSpout{


        private static final Log LOG = LogFactory.getLog(EventSpout.class);
        private static final long serialVersionUID = 886149197481637894L;
        private SpoutOutputCollector spoutOutputCollector;
        private Random random;
        private String[] records;


        public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
            this.spoutOutputCollector = spoutOutputCollector;
            random = new Random();
            records = new String[]{
                    "10001     ef2da82d4c8b49c44199655dc14f39f6     4.2.1     HUAWEI G610-U00     HUAWEI     2     70:72:3c:73:8b:22     2014-10-13 12:36:35",
                    "10001     ffb52739a29348a67952e47c12da54ef     4.3     GT-I9300     samsung     2     50:CC:F8:E4:22:E2     2014-10-13 12:36:02",
                    "10001     ef2da82d4c8b49c44199655dc14f39f6     4.2.1     HUAWEI G610-U00     HUAWEI     2     70:72:3c:73:8b:22     2014-10-13 12:36:35"
            };
        }

        public void nextTuple() {
            Utils.sleep(1000);
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss");
            Date d = new Date(System.currentTimeMillis());
            String minute = df.format(d);
            String record = records[random.nextInt(records.length)];
            LOG.info("EMIT[spout -> hdfs]" + minute + " : " + record);
            spoutOutputCollector.emit(new Values(minute, record));
        }

        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("minute", "record"));
        }
    }

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, InterruptedException, AuthorizationException{

        RecordFormat format = new DelimitedRecordFormat().withFieldDelimiter(" : ");

        SyncPolicy syncPolicy = new CountSyncPolicy(1000);

        FileRotationPolicy rotationPolicy = new TimedRotationPolicy(1.0f, TimedRotationPolicy.TimeUnit.MINUTES);

        FileNameFormat fileNameFormat = new DefaultFileNameFormat()
                .withPath("/hadoop/storm").withPrefix("app_").withExtension(".log");

        HdfsBolt hdfsBolt = new HdfsBolt()
                .withFsUrl("hdfs://ubuntu-01:9000")
                .withFileNameFormat(fileNameFormat)
                .withRecordFormat(format)
                .withRotationPolicy(rotationPolicy)
                .withSyncPolicy(syncPolicy);
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("event-spout", new EventSpout(), 3);
        builder.setBolt("hdfs-bolt", hdfsBolt, 3).fieldsGrouping("event-spout", new Fields("minute"));

        Config config = new Config();

        String name = StormToHDFSTopology.class.getSimpleName();
        if(args != null && args.length > 0){
            config.put(Config.NIMBUS_HOST, args[0]);
            config.setNumWorkers(3);
            StormSubmitter.submitTopologyWithProgressBar(name, config, builder.createTopology());
        } else{
            config.setMaxTaskParallelism(3);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(name, config, builder.createTopology());
            cluster.shutdown();
        }
    }
}
