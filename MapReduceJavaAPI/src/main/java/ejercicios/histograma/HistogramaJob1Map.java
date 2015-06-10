package ejercicios.histograma;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import ejercicios.writables.MaxMinDoubleWritable;

public class HistogramaJob1Map extends Mapper<LongWritable, Text, LongWritable, MaxMinDoubleWritable> {
	
	MaxMinDoubleWritable maxMinWritable = new MaxMinDoubleWritable();

	@Override
	protected void map(LongWritable offset, Text numero, Context context) throws IOException ,InterruptedException {
		
		LongWritable numeroClave = new LongWritable(1);
		
		maxMinWritable.setMinValue(Double.parseDouble(numero.toString()));
		maxMinWritable.setMaxValue(Double.parseDouble(numero.toString()));
		
		context.write(numeroClave, maxMinWritable);
		
	};
	
}