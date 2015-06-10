package ejercicios.histograma;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import ejercicios.writables.MaxMinDoubleWritable;

/**
 * Just for showing pourposes. Use the default {@link Reducer} because it is an
 * identity reducer.
 */
public class HistogramaJob1Reducer extends Reducer<LongWritable, MaxMinDoubleWritable, LongWritable, MaxMinDoubleWritable> {

	MaxMinDoubleWritable maxMinWritable = new MaxMinDoubleWritable();
	
	protected void reduce(LongWritable numero, Iterable<MaxMinDoubleWritable> maxMinValues, Context context) throws IOException, InterruptedException {
		
		Double valueMin = Double.MAX_VALUE;
		Double valueMax = Double.MIN_VALUE;
		
		for (MaxMinDoubleWritable maxMinValue : maxMinValues) {
			
			if(maxMinValue.getMinValue() < valueMin) {
				valueMin =  maxMinValue.getMinValue();
			}
			
			if(maxMinValue.getMaxValue() > valueMax) {
				valueMax =  maxMinValue.getMaxValue();
			}
		}
		
		maxMinWritable.setMinValue(valueMin);
		maxMinWritable.setMaxValue(valueMax);
		
		context.write(numero, maxMinWritable);
	};

}