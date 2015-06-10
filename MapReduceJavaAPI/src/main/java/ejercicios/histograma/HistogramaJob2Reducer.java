package ejercicios.histograma;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Just for showing pourposes. Use the default {@link Reducer} because it is an
 * identity reducer.
 */
public class HistogramaJob2Reducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

	
	protected void reduce(IntWritable numero, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		
		int total = 0;
		
		for (IntWritable value : values) {
			total += Integer.parseInt(value.toString());
		}
		
		context.write(numero, new IntWritable(total));
	};

}