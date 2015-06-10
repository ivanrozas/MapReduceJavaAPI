package ejercicios.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/*
 * 
 */
public class MaxMinDoubleWritable implements WritableComparable<MaxMinDoubleWritable> {

	private Double minValue;
	private Double maxValue;

	public MaxMinDoubleWritable() {
	}

	public MaxMinDoubleWritable(Double minValue, Double maxValue) {
		setMinValue(minValue);
		setMaxValue(maxValue);
	}
	
	public Double getMinValue() {
		return minValue;
	}

	public void setMinValue(Double minValue) {
		this.minValue = minValue;
	}

	public Double getMaxValue() {
		return maxValue;
	}

	public void setMaxValue(Double maxValue) {
		this.maxValue = maxValue;
	}

	@Override
	public String toString() {
		return minValue + " " + maxValue;
	}
	

	// ********************************************
	// * Important methods to override from Object
	// ********************************************

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((maxValue == null) ? 0 : maxValue.hashCode());
		result = prime * result
				+ ((minValue == null) ? 0 : minValue.hashCode());
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
		MaxMinDoubleWritable other = (MaxMinDoubleWritable) obj;
		if (maxValue == null) {
			if (other.maxValue != null)
				return false;
		} else if (!maxValue.equals(other.maxValue))
			return false;
		if (minValue == null) {
			if (other.minValue != null)
				return false;
		} else if (!minValue.equals(other.minValue))
			return false;
		return true;
	}
	
	
	// *********************
	// * Writable interface
	// *********************
	public void readFields(DataInput in) throws IOException {
		minValue = in.readDouble();
		maxValue = in.readDouble();
	}

	public void write(DataOutput out) throws IOException {
		out.writeDouble(minValue);
		out.writeDouble(maxValue);
	}

	// *********************
	// * Comparable interface
	// *********************
	@Override
	public int compareTo(MaxMinDoubleWritable o) {
		Double thisValueMin = this.minValue;
		Double thatValueMin = o.minValue;
		return (thisValueMin < thatValueMin ? -1 : (thisValueMin == thatValueMin ? 0 : 1));
	}
}
