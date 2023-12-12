package phase1;

import model.PatternInfo;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SentimentGroupComparator extends WritableComparator {

  public SentimentGroupComparator() {
    super(PatternInfo.class,true);
  }

  @Override
  public int compare(WritableComparable a, WritableComparable b) {
    PatternInfo patternInfo1 = (PatternInfo) a;
    PatternInfo patternInfo2 = (PatternInfo) b;
    return patternInfo1.compareTo(patternInfo2);
  }
}
