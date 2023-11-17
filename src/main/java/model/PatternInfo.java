package model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

public class PatternInfo implements WritableComparable<PatternInfo> {

  private PatternType patternType;
  private String patternValue;

  public PatternInfo() {
  }

  public PatternInfo(PatternType patternType, String patternValue) {
    this.patternType = patternType;
    this.patternValue = patternValue;
  }

  @Override
  public int compareTo(PatternInfo patternInfo) {
    int compareType = this.patternType.compareTo(patternInfo.getPatternType());
    if(compareType != 0)
      return compareType;
    return this.patternValue.compareTo(patternInfo.getPatternValue());
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeUTF(patternType.name());
    dataOutput.writeUTF(patternValue);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    patternType = PatternType.valueOf(dataInput.readUTF());
    patternValue = dataInput.readUTF();
  }

  public PatternType getPatternType() {
    return patternType;
  }

  public String getPatternValue() {
    return patternValue;
  }

  @Override
  public String toString() {
    return "PatternInfo{" +
        "patternType=" + patternType +
        ", patternValue='" + patternValue + '\'' +
        '}';
  }
}
