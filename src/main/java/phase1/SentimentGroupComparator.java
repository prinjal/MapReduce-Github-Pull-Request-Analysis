package phase1;

import model.PatternInfo;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SentimentGroupComparator extends WritableComparator {
    protected SentimentGroupComparator() {
        super(PatternInfo.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        PatternInfo patternInfo1 = (PatternInfo) w1;
        PatternInfo patternInfo2 = (PatternInfo) w2;

        // Compare based on PatternType
        return patternInfo1.getPatternType().compareTo(patternInfo2.getPatternType());
    }
}
