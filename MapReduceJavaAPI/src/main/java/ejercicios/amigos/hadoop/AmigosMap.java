package ejercicios.amigos.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import ejercicios.writables.RelacionAmigosWritable;

/**
 * 
 * @author Ivan Rozas
 *
 */
public class AmigosMap extends Mapper<LongWritable, Text, Text, RelacionAmigosWritable> {
	
	RelacionAmigosWritable relacionAmigosWritable = new RelacionAmigosWritable();

	@Override
	protected void map(LongWritable offset, Text amistad, Context context) throws IOException ,InterruptedException {
		
		
		String amigos[] = amistad.toString().split(" ");
		
		// Directa
		relacionAmigosWritable.setEsAmigo(true);
		relacionAmigosWritable.setAmigo(amigos[1]);
		
		context.write(new Text(amigos[0]), relacionAmigosWritable);
		
		
		// Inversa
		relacionAmigosWritable.setEsAmigo(false);
		relacionAmigosWritable.setAmigo(amigos[0]);
		
		context.write(new Text(amigos[1]), relacionAmigosWritable);
		
	};
	
}