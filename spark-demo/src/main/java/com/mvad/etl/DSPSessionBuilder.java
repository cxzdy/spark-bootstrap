package com.mvad.etl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by zhuguangbin on 16-9-2.
 */
public class DSPSessionBuilder extends Configured implements Tool {


  public static class SessionizationMapper extends TableMapper<IntWritable, Text> {

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
      Integer rowkey = ByteBuffer.wrap(value.getRow()).getInt();
      Integer usize = value.getColumnCells(Bytes.toBytes("u"), Bytes.toBytes("raw")).size();
      Integer ssize = value.getColumnCells(Bytes.toBytes("s"), Bytes.toBytes("raw")).size();
      Integer csize = value.getColumnCells(Bytes.toBytes("c"), Bytes.toBytes("raw")).size();

      IntWritable row = new IntWritable(rowkey);
      Text text = new Text(usize + "\t" + ssize + "\t" + csize);
      context.write(row, text);
    }
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new DSPSessionBuilder(), args);
  }


  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.addResource("hbase-site.xml");
    conf.set("hbase.rootdir","hdfs://ss-hadoop/hbase");

    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: DSPSessionBuilder <snapshotname> <outdir>");
      System.exit(2);
    }
    String snapshot = otherArgs[0];
    Path outPath = new Path(otherArgs[1]);

    System.out.println("snapshot:" + snapshot + ", outdir: " + outPath.toString());

    Job job = Job.getInstance(conf);

    job.setJarByClass(DSPSessionBuilder.class);
    job.setJobName("DSPSessionBuilder");

    Scan scan = new Scan();
    scan.setMaxVersions();
    scan.setBatch(10000);

    TableMapReduceUtil.initTableSnapshotMapperJob(snapshot,scan,SessionizationMapper.class, IntWritable.class, Text.class, job, true, new Path("/tmp/dspsessionbuilder"));

    FileOutputFormat.setOutputPath(job, outPath);
    return job.waitForCompletion(true) ? 0 : 1;
  }
}
