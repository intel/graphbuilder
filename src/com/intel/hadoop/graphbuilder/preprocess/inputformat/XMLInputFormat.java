package com.intel.hadoop.graphbuilder.preprocess.inputformat;

import java.io.DataInputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.log4j.Logger;

/**
 * Builtin InputFormat for XML, borrowed from Cloud9: {@link https
 * ://github.com/lintool
 * /Cloud9/blob/master/src/dist/edu/umd/cloud9/collection/XMLInputFormat.java}.
 * The class recognizes begin-of-document and end-of-document tags only:
 * everything between those delimiting tags is returned in an uninterpreted Text
 * object.
 */
public class XMLInputFormat extends TextInputFormat {
  /** Define start tag of a complete input entry. */
  public static final String START_TAG_KEY = "xmlinput.start";
  /** Define end tag of a complete input entry. */
  public static final String END_TAG_KEY = "xmlinput.end";

  @Override
  public void configure(JobConf jobConf) {
    super.configure(jobConf);
  }

  @Override
  public RecordReader<LongWritable, Text> getRecordReader(
      InputSplit inputSplit, JobConf jobConf, Reporter reporter)
      throws IOException {
    return new XMLRecordReader((FileSplit) inputSplit, jobConf);
  }

  /**
   * RecordReader for XML documents Recognizes begin-of-document and
   * end-of-document tags only: Returning text object of everything in between
   * delimiters
   */
  public static class XMLRecordReader implements
      RecordReader<LongWritable, Text> {
    private static final Logger LOG = Logger.getLogger(XMLRecordReader.class);

    private byte[] startTag;
    private byte[] endTag;
    private long start;
    private long end;
    private long pos;
    private DataInputStream fsin = null;
    private DataOutputBuffer buffer = new DataOutputBuffer();

    private long recordStartPos;

    public XMLRecordReader(FileSplit split, JobConf jobConf) throws IOException {
      if (jobConf.get(START_TAG_KEY) == null
          || jobConf.get(END_TAG_KEY) == null)
        throw new RuntimeException("Error! XML start and end tags unspecified!");

      startTag = jobConf.get(START_TAG_KEY).getBytes("utf-8");
      endTag = jobConf.get(END_TAG_KEY).getBytes("utf-8");

      start = split.getStart();
      Path file = split.getPath();

      CompressionCodecFactory compressionCodecs = new CompressionCodecFactory(
          jobConf);
      CompressionCodec codec = compressionCodecs.getCodec(file);

      FileSystem fs = file.getFileSystem(jobConf);

      if (codec != null) {
        LOG.info("Reading compressed file...");

        fsin = new DataInputStream(codec.createInputStream(fs.open(file)));

        end = Long.MAX_VALUE;
      } else {
        LOG.info("Reading uncompressed file...");

        FSDataInputStream fileIn = fs.open(file);

        fileIn.seek(start);
        fsin = fileIn;

        end = start + split.getLength();
      }

      recordStartPos = start;

      // Because input streams of gzipped files are not seekable (specifically,
      // do not support
      // getPos), we need to keep track of bytes consumed ourselves.
      pos = start;
    }

    @Override
    public boolean next(LongWritable key, Text value) throws IOException {
      if (pos < end) {
        if (readUntilMatch(startTag, false)) {
          recordStartPos = pos - startTag.length;

          try {
            buffer.write(startTag);
            if (readUntilMatch(endTag, true)) {
              key.set(recordStartPos);
              value.set(buffer.getData(), 0, buffer.getLength());
              return true;
            }
          } finally {
            // Because input streams of gzipped files are not seekable
            // (specifically, do not support
            // getPos), we need to keep track of bytes consumed ourselves.

            // This is a sanity check to make sure our internal computation of
            // bytes consumed is
            // accurate. This should be removed later for efficiency once we
            // confirm that this code
            // works correctly.
            if (fsin instanceof Seekable) {
              if (pos != ((Seekable) fsin).getPos()) {
                // throw new RuntimeException("bytes consumed error!");
                LOG.info("bytes conusmed error: " + String.valueOf(pos)
                    + " != " + String.valueOf(((Seekable) fsin).getPos()));
              }
            }

            buffer.reset();
          }
        }
      }
      return false;
    }

    @Override
    public LongWritable createKey() {
      return new LongWritable();
    }

    @Override
    public Text createValue() {
      return new Text();
    }

    @Override
    public long getPos() throws IOException {
      return pos;
    }

    @Override
    public void close() throws IOException {
      fsin.close();
    }

    @Override
    public float getProgress() throws IOException {
      return ((float) (pos - start)) / ((float) (end - start));
    }

    public long getStart() {
      return start;
    }

    public long getEnd() {
      return end;
    }

    private boolean readUntilMatch(byte[] match, boolean withinBlock)
        throws IOException {
      int i = 0;
      while (true) {
        int b = fsin.read();
        // Increment position (bytes consumed).
        pos++;

        // End of file:
        if (b == -1) {
          return false;
        }
        // Save to buffer:
        if (withinBlock) {
          buffer.write(b);
        }
        // Check if we're matching:
        if (b == match[i]) {
          i++;
          if (i >= match.length) {
            return true;
          }
        } else {
          i = 0;
        }
        // See if we've passed the stop point:
        if (!withinBlock && i == 0 && pos >= end) {
          return false;
        }
      }
    }
  }
}
