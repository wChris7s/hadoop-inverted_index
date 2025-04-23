package cw.ucsp.app;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
  @Override
  protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
    Map<String, Integer> map = new HashMap<>();

    for (Text value: values) {
      if (map.containsKey(value.toString())) {
        map.put(value.toString(), map.get(value.toString()) + 1);
      } else {
        map.put(value.toString(), 1);
      }
    }

    StringBuilder docs = new StringBuilder();
    for (String docId : map.keySet()) {
      String doc = String.format("%s : %d ", docId, map.get(docId));
      docs.append(doc);
    }

    context.write(key, new Text(docs.toString()));
  }
}
