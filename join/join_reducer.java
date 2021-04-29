package join;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class join_reducer extends Reducer<Text,Text,Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<String> grandparents = new ArrayList<String>();
        List<String> child = new ArrayList<String>();
        for (Text value : values) {
            if (value.toString().startsWith("-")){
                grandparents.add(value.toString().substring(1));
            }else{
                child.add(value.toString());
            }
        }

        if (grandparents.size()!=0 && child.size()!=0){
            for (int i = 0; i < grandparents.size(); i++) {
                for (int i1 = 0; i1 < child.size(); i1++) {
                    context.write(new Text(grandparents.get(i)),new Text(child.get(i1)));
                }
            }
        }
    }
}
