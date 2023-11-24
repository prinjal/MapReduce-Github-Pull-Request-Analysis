package phase1;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations.SentimentAnnotatedTree;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;
import model.DateInfo;
import model.PatternInfo;
import model.PatternType;
import model.SentimentInfo;
import model.SentimentType;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.io.StringReader;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.WeekFields;
import java.util.Locale;
import java.util.Properties;

public class SentimentMapper extends Mapper<LongWritable, Text, PatternInfo, SentimentInfo> {
  private StanfordCoreNLP pipeline;
  private DateTimeFormatter formatter;

  private CSVFormat csvFormat;

  @Override
  protected void setup(Mapper<LongWritable, Text, PatternInfo, SentimentInfo>.Context context) {
    formatter = DateTimeFormatter.ofPattern(util.Constants.COMMIT_DATE_PATTERN);
    Properties props = new Properties();
    props.setProperty("annotators", "tokenize, parse, sentiment");
    pipeline = new StanfordCoreNLP(props);
    csvFormat=CSVFormat.EXCEL.builder()
            .setRecordSeparator(',')
            .setSkipHeaderRecord(true)
            .setIgnoreSurroundingSpaces(true).build();
  }

  @Override
  protected void map(LongWritable key, Text value,
                     Mapper<LongWritable, Text, PatternInfo, SentimentInfo>.Context context)
          throws InterruptedException {
    try {
      StringReader stringReader = new StringReader(value.toString());
      CSVParser csvParser = csvFormat.parse(stringReader);
      for (CSVRecord record : csvParser) {

        String comment = record.get(util.Constants.COMMENT_INDEX);

        SentimentType sentimentType = getSentiment(comment);
        SentimentInfo sentimentInfo = new SentimentInfo(sentimentType);
        writeRecord(PatternType.LANGUAGE, record.get(util.Constants.LANGUAGE_INDEX), sentimentInfo, context);
        writeRecord(PatternType.AUTHOR, record.get(util.Constants.AUTHOR_INDEX), sentimentInfo, context);

        String commitDate = record.get(util.Constants.COMMIT_DATE_INDEX);
        if (commitDate != null) {
          LocalDate date = LocalDate.parse(commitDate, formatter);
          int year = date.getYear();
          String month = date.getMonth().toString();
          int week = date.get(WeekFields.of(Locale.US).weekOfWeekBasedYear());
          writeRecord(PatternType.YEARLY, new DateInfo(year).toString(), sentimentInfo, context);
          writeRecord(PatternType.MONTHLY, new DateInfo(year, month).toString(), sentimentInfo, context);
          writeRecord(PatternType.WEEKLY, new DateInfo(year, month, week).toString(), sentimentInfo, context);
        }
      }
      csvParser.close();
    } catch (IOException | RuntimeException e) {
      e.printStackTrace();
    }
  }

  private SentimentType getSentiment(String value) {
    int sentimentVal = 0;
    Annotation annotation = pipeline.process(value);
    for (CoreMap sentence : annotation.get(CoreAnnotations.SentencesAnnotation.class)) {
      Tree tree = sentence.get(SentimentAnnotatedTree.class);
      sentimentVal = RNNCoreAnnotations.getPredictedClass(tree);
    }

    if (sentimentVal < 2)
      return SentimentType.NEGATIVE;
    else if (sentimentVal == 2)
      return SentimentType.NEUTRAL;
    else
      return SentimentType.POSITIVE;
  }

  private void writeRecord(PatternType patternType, String patternValue, SentimentInfo sentimentInfo, Context context)
          throws IOException, InterruptedException {
    if (patternValue == null || patternValue.trim().equals(""))
      return;
    PatternInfo patternInfo = new PatternInfo(patternType, patternValue);
    context.write(patternInfo, sentimentInfo);
  }
}
