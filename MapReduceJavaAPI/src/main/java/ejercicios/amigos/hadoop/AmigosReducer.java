package ejercicios.amigos.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import ejercicios.writables.RelacionAmigosWritable;

/**
 * 
 * @author Ivan Rozas
 *
 */
public class AmigosReducer extends Reducer<Text, RelacionAmigosWritable, Text, Text> {

	
	protected void reduce(Text amigo, Iterable<RelacionAmigosWritable> relacionAmigos, Context context) throws IOException, InterruptedException {
		
		List<String> relaccionesDirectas = new ArrayList<String>();
		List<String> relaccionesInversas = new ArrayList<String>();
		
		
		for (RelacionAmigosWritable relacionAmigo : relacionAmigos) {
			if(relacionAmigo.isEsAmigo()){
				relaccionesDirectas.add(relacionAmigo.getAmigo());
			}else{
				relaccionesInversas.add(relacionAmigo.getAmigo());
			}
		}
		
		for (String relaccioneInversa : relaccionesInversas) {
			for (String relaccionDirecta : relaccionesDirectas) {
				context.write(new Text(relaccioneInversa), new Text(relaccionDirecta));
			}
		}
		
	};

}