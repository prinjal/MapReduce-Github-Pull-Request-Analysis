package model;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AnalysisPattern implements WritableComparable<AnalysisPattern> {

    private PatternType patternType;
    private String patternValue;

    public AnalysisPattern(PatternType patternType, String patternValue) {
        this.patternType = patternType;
        this.patternValue = patternValue;
    }

    public AnalysisPattern() {
    }

    public PatternType getPatternType() {
        return patternType;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(patternType.name());
        out.writeUTF(patternValue);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        patternType = PatternType.valueOf(in.readUTF());
        patternValue = in.readUTF();
    }

    @Override
    public int compareTo(AnalysisPattern o) {
        return this.patternType.compareTo(o.getPatternType());
    }

    @Override
    public String toString() {
        return "AnalysisPattern{" +
                "patternType=" + patternType +
                ", patternValue='" + patternValue + '\'' +
                '}';
    }
}
