package ejercicios.amigos.pangool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Schema.Field;
import com.datasalt.pangool.io.Schema.Field.Type;
import com.datasalt.pangool.io.Tuple;
import com.datasalt.pangool.tuplemr.TupleMRBuilder;
import com.datasalt.pangool.tuplemr.TupleMRException;
import com.datasalt.pangool.tuplemr.TupleMapper;
import com.datasalt.pangool.tuplemr.TupleReducer;
import com.datasalt.pangool.tuplemr.mapred.lib.input.HadoopInputFormat;
import com.datasalt.pangool.tuplemr.mapred.lib.output.HadoopOutputFormat;

/**
 * 
 * @author Ivan Rozas
 * 
 */
public class AmigosJobPangool extends Configured implements Tool {

	public static Schema getSchema() {
		List<Field> fields = new ArrayList<Field>();
		fields.add(Field.create("amigo1", Type.STRING));
		fields.add(Field.create("esAmigo", Type.BOOLEAN));
		fields.add(Field.create("amigo2", Type.STRING));
		return new Schema("schema", fields);
	}

	@SuppressWarnings("serial")
	public static class RelaccionesMap extends TupleMapper<LongWritable, Text> {

		private Tuple tuple = new Tuple(getSchema());

		@Override
		public void map(LongWritable key, Text amistad, TupleMRContext context, Collector collector) throws IOException, InterruptedException {

			String amigos[] = amistad.toString().split(" ");

			tuple.set("amigo1", amigos[0]);
			tuple.set("esAmigo", true);
			tuple.set("amigo2", amigos[1]);
			collector.write(tuple);

			tuple.set("amigo1", amigos[1]);
			tuple.set("esAmigo", false);
			tuple.set("amigo2", amigos[0]);
			collector.write(tuple);

		}
	}

	@SuppressWarnings("serial")
	public static class AmigosReduce extends TupleReducer<Text, Text> {

		@Override
		public void reduce(ITuple group, Iterable<ITuple> tuples, TupleMRContext context, Collector collector) throws IOException, InterruptedException, TupleMRException {

			List<String> relaccionesDirectas = new ArrayList<String>();
			List<String> relaccionesInversas = new ArrayList<String>();
			
			for (ITuple tuple : tuples) {
				if(tuple.get("esAmigo").equals(true)){
					relaccionesDirectas.add(tuple.get("amigo2").toString());
				}else{
					relaccionesInversas.add(tuple.get("amigo2").toString());
				}
			}

			for (String relaccionInversa : relaccionesInversas) {
				for (String relaccionDirecta : relaccionesDirectas) {
					collector.write(new Text(relaccionInversa), new Text(relaccionDirecta));
				}
			}
			
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		
		if (args.length != 2) {
			System.out.println("Invalid number of arguments\n\n" + "Usage: Amigos-pangool <input_path> <output_path>\n\n");
			return -1;
		}
		
		String input = args[0];
		String output = args[1];

		Path oPath = new Path(output);
		FileSystem.get(oPath.toUri(), getConf()).delete(oPath, true);

		TupleMRBuilder mr = new TupleMRBuilder(getConf(), "Amigos Pangool");
		mr.setJarByClass(AmigosJobPangool.class);
		
		mr.addIntermediateSchema(getSchema());
		mr.setGroupByFields("amigo1");
		mr.setTupleReducer(new AmigosReduce());
		
		mr.addInput(new Path(input), new HadoopInputFormat(TextInputFormat.class), new RelaccionesMap());
		mr.setOutput(new Path(output), new HadoopOutputFormat(TextOutputFormat.class), Text.class, NullWritable.class);
		
		mr.createJob().waitForCompletion(true);

		return 0;
	}

	public static void main(String args[]) throws Exception {
		ToolRunner.run(new AmigosJobPangool(), args);
	}
}
