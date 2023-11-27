package model;

import org.apache.commons.lang.ObjectUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AnalysisSentiment implements Writable {

    private AnalysisPattern analysisPattern;
    private SentimentPercent sentimentPercent;

    public AnalysisSentiment() {
    }

    public AnalysisSentiment(AnalysisPattern analysisPattern, SentimentPercent sentimentPercent) {
        this.analysisPattern = analysisPattern;
        this.sentimentPercent = sentimentPercent;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.analysisPattern.write(out);
        this.sentimentPercent.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        try {
            this.analysisPattern=new AnalysisPattern(PatternType.valueOf(in.readUTF()),in.readUTF());
            this.sentimentPercent=new SentimentPercent(in.readDouble(),in.readDouble(),in.readDouble());
        }
        catch (NullPointerException e){
            e.printStackTrace();
        }

    }

    @Override
    public String toString() {
        return "AnalysisSentiment{" +
                "analysisPattern=" + analysisPattern +
                ", sentimentPercent=" + sentimentPercent +
                '}';
    }
}
