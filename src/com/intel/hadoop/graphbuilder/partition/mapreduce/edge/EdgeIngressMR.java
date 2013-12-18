/* Copyright (C) 2012 Intel Corporation.
 *     All rights reserved.
 *           
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 * For more about this software visit:
 *      http://www.01.org/GraphBuilder 
 */
package com.intel.hadoop.graphbuilder.partition.mapreduce.edge;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.log4j.Logger;

import com.intel.hadoop.graphbuilder.graph.GraphOutput;
import com.intel.hadoop.graphbuilder.graph.simplegraph.SimpleGraphOutput;
import com.intel.hadoop.graphbuilder.parser.FieldParser;
import com.intel.hadoop.graphbuilder.parser.GraphParser;
import com.intel.hadoop.graphbuilder.partition.mapreduce.keyvalue.IngressKeyType;
import com.intel.hadoop.graphbuilder.partition.mapreduce.keyvalue.IngressValueType;

/**
 * The MapRedue class takes from input directory a list of edges and vertices,
 * and output 2 parts: partitioned graphs and a list of distributed vertex
 * records.
 * <p>
 * Input directory: Can take multiple input directories containing list of
 * edges. Output directory structure:
 * <ul>
 * <li>$outputdir/partition{$i}/subpart{$j}/edata for edge data.</li>
 * <li>Metafile: $outputdir/partition{$i}/subpart{$j} for meta info.</li>
 * <li>Graph structure: $outputdir/partition{$i}/subpart{$j}/edgelist for
 * adjacency structure.</li>
 * <li>VertexRecords: $outputdir/vrecord list of vertex records.</li>
 * </ul>
 * </p>
 * 
 */
public class EdgeIngressMR {

  private static final Logger LOG = Logger.getLogger(EdgeIngressMR.class);

  public static int IngressCodeLimit = 4;

  /** MapReduce Job Counters. */
  public static enum COUNTER {
    NUM_VERTICES, NUM_EDGES
  };

  /**
   * Default constructor, initialize with parsers.
   * 
   * @param graphparser
   * @param vidparser
   * @param vdataparser
   * @param edataparser
   */
  public EdgeIngressMR(Class graphparser, Class vidparser, Class vdataparser,
      Class edataparser) {
    gzip = false;
    jobName = "Ingress Mapreduce Driver";
    setParser(graphparser, vidparser, vdataparser, edataparser);
    conf = new JobConf(EdgeIngressMR.class);
  }

  /**
   * Set the parser class.
   * 
   * @param parser
   */
  public void setParser(Class graphparser, Class vidparser, Class vdataparser,
      Class edataparser) {
    try {
      this.graphparser = (GraphParser) graphparser.newInstance();
      this.vidparser = (FieldParser) vidparser.newInstance();
      this.vdataparser = (FieldParser) vdataparser.newInstance();
      this.edataparser = (FieldParser) edataparser.newInstance();
    } catch (InstantiationException e) {
      e.printStackTrace();
      LOG.fatal("Parser classes: \n" + graphparser + "\n" + vidparser + "\n"
          + vdataparser + "\n" + edataparser + " do not exist.");
    } catch (IllegalAccessException e) {
      e.printStackTrace();
      LOG.fatal("Parser classes: \n" + graphparser + "\n" + vidparser + "\n"
          + vdataparser + "\n" + edataparser + " do not exist.");
    }
  }

  /**
   * Set the job name.
   * 
   * @param name
   */
  public void setJobName(String name) {
    this.jobName = name;
  }

  /**
   * Set option for using gzip compression in output.
   * 
   * @param gzip
   */
  public void useGzip(boolean gzip) {
    this.gzip = gzip;
  }

  /**
   * Set the ingress strategy {random, oblivious}.
   * 
   * @see {ObliviousIngress}
   * @see {RandomIngress}
   * @param ingress
   */
  public void setIngress(String ingress) {
    if (ingress.equals("random") || ingress.equals("greedy")
        || ingress.equals("constrainedgreedy")
        || ingress.equals("constrainedrandom")
        || ingress.equals("constrainedpdsrandom")
        || ingress.equals("constrainedtorusrandom")
        || ingress.equals("constrainedtorusgreedy")
        )
      this.ingress = ingress;
    else {
      LOG.error("Unknown ingress method: " + ingress
          + "\n Supported ingress methods: oblivious, random," 
          + "constrainedgreedy, constrainedrandom, constrainedpdsrandom,"
          +"constrainedtorusrandom, constrainedtorusgreedy");
      LOG.error("Use the default oblivious ingress");
      this.ingress = "greedy";
    }
  }

  /**
   * Set the intermediate key value class.
   * 
   * @param keyClass
   * @param valClass
   */
  public void setKeyValueClass(Class keyClass, Class valClass) {
    try {
      this.mapkeytype = (IngressKeyType) keyClass.newInstance();
      this.mapvaltype = (IngressValueType) valClass.newInstance();
    } catch (InstantiationException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    }
  }

  /**
   * @return JobConf of the current job.
   */
  public JobConf getConf() {
    return conf;
  }


  /**
   * @param inputpath
   * @param outputpath
   * @param numProcs
   * @param ingress
   * @throws IOException
   */
  public void run(String[] inputpaths, String outputpath, int numProcs,
      int ingressCode) throws IOException {
    conf.setJobName(jobName);
    if (this.subpartPerPartition <= 0)
      this.subpartPerPartition = 8;

	switch (ingressCode) {
		case 0:
			this.ingress = "random";
			break;
		case 1:
			this.ingress = "greedy";
			break;
		case 2:
			this.ingress = "constrainedrandom";
			break;
		case 3:
			this.ingress = "constrainedgreedy";
			break;
		case 4:
			this.ingress = "constrainedpdsrandom";
			break;
	}

    this.setIngress(this.ingress);

    LOG.info("===== Job: Partition edges and create vertex records =========");
    LOG.info("input: " + StringUtils.join(inputpaths, ","));
    LOG.info("output: " + outputpath);
    LOG.info("numProc = " + numProcs);
    LOG.info("subpartPerPartition = " + subpartPerPartition);
    LOG.info("keyclass = " + this.mapkeytype.getClass().getName());
    LOG.info("valclass = " + this.mapvaltype.getClass().getName());
    LOG.debug("graphparser = " + this.graphparser.getClass().getName());
    LOG.debug("vidparser = " + this.vidparser.getClass().getName());
    LOG.debug("vdataparser = " + this.vdataparser.getClass().getName());
    LOG.debug("edataparser = " + this.edataparser.getClass().getName());
    LOG.info("ingress = " + this.ingress);
    LOG.info("gzip = " + Boolean.toString(gzip));
    
    if (this.ingress.equals("constrainedpdsrandom")) {
        int prime = (int)Math.floor(Math.sqrt(numProcs-1));
        if (numProcs != (prime*prime + prime + 1)){
            LOG.info("numProcs does not meet requirement.Ingress switchs to constrainedrandom");
        }
    }

	conf.set("ingress", this.ingress);
	conf.setInt("numProcs", numProcs);
    conf.set("GraphParser", graphparser.getClass().getName());
    conf.set("VidParser", vidparser.getClass().getName());
    conf.set("VdataParser", vdataparser.getClass().getName());
    conf.set("EdataParser", edataparser.getClass().getName());
    conf.setInt("subpartPerPartition", subpartPerPartition);

    //if ((vidparser.getClass().getName().toLowerCase().endsWith("intparser")
    //    || vidparser.getClass().getName().toLowerCase().endsWith("longparser"))
    //    && (edataparser.getClass().getName().toLowerCase().endsWith("intparser")
    //    || edataparser.getClass().getName().toLowerCase().endsWith("longparser")
    //    || edataparser.getClass().getName().toLowerCase().endsWith("floatparser")))
    //{
    //     //should refine here
    //     LOG.info("Use fast utility library for managing large graph");
    //     conf.setInt("useFastUtil", 1);
    // }

     LOG.info("===============================================================");

    conf.setMapOutputKeyClass(this.mapkeytype.getClass());
    conf.setMapOutputValueClass(this.mapvaltype.getClass());

    conf.setOutputKeyClass(IntWritable.class);
    conf.setOutputValueClass(Text.class);

    conf.setMapperClass(EdgeIngressMapper.class);
    conf.setCombinerClass(EdgeIngressCombiner.class);
    conf.setReducerClass(EdgeIngressReducer.class);

    // GraphOutput output = new GLGraphOutput();
    GraphOutput output = new SimpleGraphOutput();
    output.init(conf);

    conf.setInputFormat(TextInputFormat.class);
    // conf.setOutputFormat(PartitionedGraphOutputFormat.class);

    if (gzip) {
      TextOutputFormat.setCompressOutput(conf, true);
      TextOutputFormat.setOutputCompressorClass(conf, GzipCodec.class);
    }

    for (String path : inputpaths)
      FileInputFormat.addInputPath(conf, new Path(path));
    FileOutputFormat.setOutputPath(conf, new Path(outputpath));

    if (!checkTypes()) {
      LOG.fatal("Type check failed."
          + "Please check the parsers are consistent with key/val types.");
      return;
    }

    JobClient.runJob(conf);
    LOG.info("================== Done ====================================\n");
  }

  /**
   * Ensure the keytype, valuetype are consistent with the parser type.
   * @return true if type check.
   */
  private boolean checkTypes() {
    boolean check = true;

    if (!(mapkeytype.createVid().getClass()).equals(mapvaltype
        .getGraphTypeFactory().createVid().getClass())) {
      LOG.fatal("VidType is not consistant between MapKeyType: "
          + mapkeytype.createVid().getClass().getName() + " and MapValueType: "
          + mapvaltype.getGraphTypeFactory().createVid().getClass().getName());
      check = false;
    }

    if (!(vidparser.getType()).equals(mapkeytype.createVid().getClass())) {
      LOG.fatal("VidType is not consistant between MapKeyType: "
          + mapkeytype.createVid().getClass().getName() + " and Parser: "
          + vidparser.getType().getName());
      check = false;
    }

    if (!(vdataparser.getType().equals(mapvaltype.getGraphTypeFactory()
        .createVdata().getClass()))) {
      LOG.fatal("VertexDataType is not consistant between MapValueType: "
          + mapvaltype.getGraphTypeFactory().createVdata().getClass().getName()
          + " and Parser: " + vdataparser.getType().getName());
      check = false;
    }

    if (!(edataparser.getType().equals(mapvaltype.getGraphTypeFactory()
        .createEdata().getClass()))) {
      LOG.fatal("EdgeDataType is not consistant between MapValueType: "
          + mapvaltype.getGraphTypeFactory().createEdata().getClass().getName()
          + " and Parser: " + edataparser.getType().getName());
      check = false;
    }

    return check;
  }

  /**
   * Set the number of subpartitions per real partition.
   * @param n number of subpartitions per real partition.
   */
  public void setTotalSubPartition(int n) {
    this.subpartPerPartition = n;
  }

  private JobConf   conf;
  private GraphParser graphparser;
  private FieldParser vidparser;
  private FieldParser vdataparser;
  private FieldParser edataparser;

  private boolean gzip;
  private String jobName;
  private String ingress;
  private int subpartPerPartition;
  private IngressKeyType mapkeytype;
  private IngressValueType mapvaltype;
}
