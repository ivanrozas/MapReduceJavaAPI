package ejercicios.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * 
 * @author Ivan Rozas
 *
 */
public class RelacionAmigosWritable implements WritableComparable<RelacionAmigosWritable> {

	private boolean esAmigo;
	private String amigo;

	public RelacionAmigosWritable() {
	}

	public RelacionAmigosWritable(boolean esAmigo, String amigo) {
		setEsAmigo(esAmigo);
		setAmigo(amigo);
	}


	public boolean isEsAmigo() {
		return esAmigo;
	}

	public void setEsAmigo(boolean esAmigo) {
		this.esAmigo = esAmigo;
	}

	public String getAmigo() {
		return amigo;
	}

	public void setAmigo(String amigo) {
		this.amigo = amigo;
	}

	@Override
	public String toString() {
		return "RelacionAmigosWritable [esAmigo=" + esAmigo + ", amigo=" + amigo + "]";
	}

	// ********************************************
	// * Important methods to override from Object
	// ********************************************

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((amigo == null) ? 0 : amigo.hashCode());
		result = prime * result + (esAmigo ? 1231 : 1237);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		RelacionAmigosWritable other = (RelacionAmigosWritable) obj;
		if (amigo == null) {
			if (other.amigo != null)
				return false;
		} else if (!amigo.equals(other.amigo))
			return false;
		if (esAmigo != other.esAmigo)
			return false;
		return true;
	}

	
	// *********************
	// * Writable interface
	// *********************
	public void readFields(DataInput in) throws IOException {
		esAmigo = in.readBoolean();
		amigo = in.readUTF();
	}

	public void write(DataOutput out) throws IOException {
		out.writeBoolean(esAmigo);
		out.writeUTF(amigo);
	}

	// *********************
	// * Comparable interface
	// *********************
	@Override
	public int compareTo(RelacionAmigosWritable o) {
		boolean thisValueMin = this.esAmigo;
		boolean thatValueMin = o.esAmigo;
		return (thisValueMin == thatValueMin ? -1 : (thisValueMin == thatValueMin ? 0 : 1));
	}
}
