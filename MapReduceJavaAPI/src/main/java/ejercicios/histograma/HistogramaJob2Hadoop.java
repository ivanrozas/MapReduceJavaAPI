package ejercicios.histograma;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * 
 * @author cloudera
 *
 */
public class HistogramaJob2Hadoop extends Configured implements Tool {

	private static final String NOMBRE_FICHERO_SALIDA = "part-r-00000";
	
	
	@Override
	public int run(String[] args) throws Exception {
		
		if (args.length != 4) {
			System.out.println("Invalid number of arguments\n\n" + "Usage: Histograma-hadoop-job2 <input_path> <output_path_Job1> <output_path_Job2> <number_colum>\n\n");
			return -1;
		}
		
		
		String input = args[0];
		String outputJob1 = args[1] + "/" + NOMBRE_FICHERO_SALIDA;
		String outputJob2 = args[2];
		String numColumnas = args[3];

		// Se pasa comno parametro el numero de coumnosa
		getConf().set("numColumnas", numColumnas);
		getConf().set("ficheroOutJob1", outputJob1);
		
		// Se borra el fichero por si existe que no de fallo
		Path oPath = new Path(outputJob2);
		FileSystem.get(oPath.toUri(), getConf()).delete(oPath, true);

		Job job = Job.getInstance(getConf(), "Histograma Hadoop Job2");
		job.setJarByClass(HistogramaJob2Hadoop.class);
		
		// input/output job
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		// Map
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);

		// Reducer
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

		// Clases del Job
		job.setMapperClass(HistogramaJob2Map.class);
		job.setCombinerClass(HistogramaJob2Reducer.class);
		job.setReducerClass(HistogramaJob2Reducer.class); 	

		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(outputJob2));
		
		job.waitForCompletion(true);

		return 0;
	}

	public static void main(String args[]) throws Exception {
		ToolRunner.run(new HistogramaJob2Hadoop(), args);
	}
}
