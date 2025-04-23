package cw.ucsp.app;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class InvertedIndexMapper extends Mapper<Object, Text, Text, Text> {

  private final Text word = new Text();

  @Override
  protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException {
    String[] data = value.toString().split("\\t", 2);
    String docId = data[0];
    String content = data[1]
      .replaceAll("[^a-zA-Z]+", " ")
      .toLowerCase();

    StringTokenizer tokenizer = new StringTokenizer(content);
    while (tokenizer.hasMoreTokens()) {
      word.set(tokenizer.nextToken());
      context.write(word, new Text(docId.trim()));
    }
  }
}
