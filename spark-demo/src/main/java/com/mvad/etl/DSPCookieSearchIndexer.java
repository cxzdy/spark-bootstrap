package com.mvad.etl;

import com.mediav.data.log.LogUtils;
import com.mediav.data.log.unitedlog.UnitedEvent;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by zhuguangbin on 16-9-2.
 */
public class DSPCookieSearchIndexer extends Configured implements Tool {

  private static final String INDEX_TABLE_CONF_KEY = "dspcookieindexer.index.table.name";


  public static class CookieIndexMapper extends TableMapper<Text, LongWritable> {

    private Table indexTable = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      Connection conn = ConnectionFactory.createConnection(context.getConfiguration());
      indexTable = conn.getTable(TableName.valueOf(context.getConfiguration().get(INDEX_TABLE_CONF_KEY)));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      indexTable.close();
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {

      for (Map.Entry<byte[], byte[]> entry : value.getFamilyMap(Bytes.toBytes("u")).entrySet()) {
        byte[] logId = entry.getKey();
        byte[] rawEvent = entry.getValue();
        UnitedEvent event = LogUtils.thriftBinarydecoder(rawEvent, UnitedEvent.class);
        if (event != null && event.isSetLinkedIds() && event.getLinkedIds().isSetMvid() && event.getLinkedIds().getMvid() != null && event.isSetAdvertisementInfo() && event.getAdvertisementInfo().isSetImpressionInfos()) {
          List<Put> puts = new ArrayList<>();
          for (int i = 0; i < event.getAdvertisementInfo().getImpressionInfosSize(); i++) {
            Long showRequestId = event.getAdvertisementInfo().getImpressionInfos().get(i).getShowRequestId();
            Put put = new Put(Bytes.toBytes(event.getLinkedIds().getMvid()));
            put.addColumn(Bytes.toBytes("u"), Bytes.toBytes("id"), event.getEventTime(), Bytes.toBytes(showRequestId));
            puts.add(put);
          }
          indexTable.put(puts);
        }
      }

      for (Map.Entry<byte[], byte[]> entry : value.getFamilyMap(Bytes.toBytes("s")).entrySet()) {
        byte[] logId = entry.getKey();
        byte[] rawEvent = entry.getValue();
        UnitedEvent event = LogUtils.thriftBinarydecoder(rawEvent, UnitedEvent.class);
        if (event != null && event.isSetLinkedIds() && event.getLinkedIds().isSetMvid() && event.getLinkedIds().getMvid() != null && event.isSetAdvertisementInfo() && event.getAdvertisementInfo().isSetImpressionInfos()) {
          List<Put> puts = new ArrayList<>();
          for (int i = 0; i < event.getAdvertisementInfo().getImpressionInfosSize(); i++) {
            Long showRequestId = event.getAdvertisementInfo().getImpressionInfos().get(i).getShowRequestId();
            Put put = new Put(Bytes.toBytes(event.getLinkedIds().getMvid()));
            put.addColumn(Bytes.toBytes("s"), Bytes.toBytes("id"), event.getEventTime(), Bytes.toBytes(showRequestId));
            puts.add(put);
          }
          indexTable.put(puts);
        }
      }

      for (Map.Entry<byte[], byte[]> entry : value.getFamilyMap(Bytes.toBytes("c")).entrySet()) {
        byte[] logId = entry.getKey();
        byte[] rawEvent = entry.getValue();
        UnitedEvent event = LogUtils.thriftBinarydecoder(rawEvent, UnitedEvent.class);
        if (event != null && event.isSetLinkedIds() && event.getLinkedIds().isSetMvid() && event.getLinkedIds().getMvid() != null && event.isSetAdvertisementInfo() && event.getAdvertisementInfo().isSetImpressionInfos()) {
          Long showRequestId = event.getAdvertisementInfo().getImpressionInfo().getShowRequestId();
          Put put = new Put(Bytes.toBytes(event.getLinkedIds().getMvid()));
          put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("id"), event.getEventTime(), Bytes.toBytes(showRequestId));
          indexTable.put(put);
        }
      }
    }
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new DSPCookieSearchIndexer(), args);
  }


  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.rootdir", "hdfs://ss-hadoop/hbase");
    conf.set("hbase.zookeeper.quorum","nn7ss.prod.mediav.com,nn8ss.prod.mediav.com,nn9ss.prod.mediav.com");

    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: DSPSessionBuilder <snapshotname> <indextable>");
      System.exit(2);
    }
    String snapshot = otherArgs[0];
    String indextable = otherArgs[1];

    System.out.println("snapshot:" + snapshot + ", indextable: " + indextable);

    conf.set(INDEX_TABLE_CONF_KEY, indextable);

    Job job = Job.getInstance(conf);

    job.setJarByClass(DSPCookieSearchIndexer.class);
    job.setJobName("DSPCookieSearchIndexer");
    job.setNumReduceTasks(0);
    FileOutputFormat.setOutputPath(job, new Path("/tmp/dspcookieindexer"));
    Scan scan = new Scan();
    scan.setBatch(10000);

    TableMapReduceUtil.initTableSnapshotMapperJob(snapshot, scan, CookieIndexMapper.class, IntWritable.class, Text.class, job, true, new Path("/tmp/dspsessionbuilder"));

    return job.waitForCompletion(true) ? 0 : 1;
  }
}
