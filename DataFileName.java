package predictionNewAC;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

class DataFileName implements WritableComparable {
	
	public String data;
	public String filename;
    

    public DataFileName() {
    }
    
    public DataFileName(String data, String filename) {
    	this.data = data;
        this.filename = filename;
    }

    public void write(DataOutput out) throws IOException {
    	out.writeUTF(data);
    	out.writeUTF(filename);
    }

    public void readFields(DataInput in) throws IOException {
    	this.data =in.readUTF();
    	this.filename =in.readUTF();
    }

	@Override
	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		return 0;
	}
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return this.data + "," + this.filename ;
	}

   

 
}

class XY implements WritableComparable {
	

    public long x;
    public long y;
    

    public XY() {
    }
    
    public XY(long x, long y) {
    	this.x = x;
    	this.y = y;
    }

    public void write(DataOutput out) throws IOException {
        out.writeLong(x);
        out.writeLong(y);
    }

    public void readFields(DataInput in) throws IOException {
        this.x = in.readLong();
        this.y = in.readLong();
    }

	@Override
	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		return 0;
	}
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return this.x + "," + this.y;
	}

   

 
}
