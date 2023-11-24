package model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class SentimentInfo implements Writable {

  private SentimentType sentimentType;
  public SentimentInfo() {
  }

  public SentimentInfo(SentimentType sentimentType) {
    this.sentimentType = sentimentType;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeUTF(sentimentType.name());
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    sentimentType = SentimentType.valueOf(dataInput.readUTF());
  }

  public SentimentType getSentimentType() {
    return sentimentType;
  }

  @Override
  public String toString() {
    return String.valueOf(sentimentType);
  }
}
