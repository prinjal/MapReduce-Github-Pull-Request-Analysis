package phase2;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvMalformedLineException;
import com.opencsv.exceptions.CsvValidationException;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import model.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.io.StringReader;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

import static util.Constants.*;

public class AnalysisMapper extends Mapper<LongWritable, Text, PatternInfo, AnalysisSentiment> {

    @Override
    protected void setup(Mapper<LongWritable, Text, PatternInfo, AnalysisSentiment>.Context context) {
    }

    protected void map(LongWritable key, Text value,
                       Mapper<LongWritable, Text, PatternInfo, AnalysisSentiment>.Context context)
            throws IOException, InterruptedException {
    try{
            CSVReader csvReader = new CSVReader(new StringReader(value.toString()));
            while(csvReader.peek() != null){
                String[] recordArr = csvReader.readNext();
                String patternType = recordArr[PATTERN_TYPE];
                String patternValue = recordArr[PATTERN_VALUE];
                String negativePercent = recordArr[NEGATIVE_PERCENT];
                String positivePercent = recordArr[POSITIVE_PERCENT];
                String neutralPercent = recordArr[NEUTRAL_PERCENT];

                PatternInfo patternInfo=new PatternInfo(PatternType.valueOf(patternType),patternValue);
                SentimentPercent sentimentPercent=new SentimentPercent(Double.parseDouble(negativePercent),
                        Double.parseDouble(positivePercent),
                        Double.parseDouble(neutralPercent));

                AnalysisSentiment analysisSentiment=new AnalysisSentiment(patternInfo,sentimentPercent);

                context.write(patternInfo,analysisSentiment);

            }
        } catch (CsvValidationException | CsvMalformedLineException | RuntimeException e) {
        e.printStackTrace();//skip errors
        }
    }

}