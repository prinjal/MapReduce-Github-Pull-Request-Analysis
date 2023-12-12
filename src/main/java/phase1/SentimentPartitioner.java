package phase1;

import java.util.Objects;
import model.PatternInfo;
import model.SentimentInfo;
import org.apache.hadoop.mapreduce.Partitioner;

public class SentimentPartitioner extends Partitioner<PatternInfo, SentimentInfo> {

  @Override
  public int getPartition(PatternInfo patternInfo, SentimentInfo sentimentInfo, int numPartition) {
    return (Objects.hash(patternInfo.getPatternType().name(), patternInfo.getPatternValue())  & Integer.MAX_VALUE) % numPartition;
  }
}
