package model;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AnalysisSentiment implements Writable {

    private PatternInfo patternInfo;
    private SentimentPercent sentimentPercent;

    public AnalysisSentiment() {
    }

    public AnalysisSentiment(PatternInfo patternInfo, SentimentPercent sentimentPercent) {
        this.patternInfo = patternInfo;
        this.sentimentPercent = sentimentPercent;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.patternInfo.write(out);
        this.sentimentPercent.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.patternInfo.readFields(in);
        this.sentimentPercent.readFields(in);
    }
}
