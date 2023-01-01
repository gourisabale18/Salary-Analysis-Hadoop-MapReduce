

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

//making use of custom key/Composite Key Class for deciding Age Group and performing sorting based on Age Groups

public class EmpGrpKey implements WritableComparable<EmpGrpKey>{
    String ageGroup;

    public EmpGrpKey()
    {
    }

    public EmpGrpKey( String ageGroup)
    {
        this.ageGroup=ageGroup;

    }

    @Override
    public String toString(){//overriding the toString() method
        return this.ageGroup;
    }

    //This method will be used to perform the sorting in descending order
    public int compareTo(EmpGrpKey o) {
        //sort in descending order
        return o.ageGroup.compareTo(ageGroup);
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {

        new Text(ageGroup).write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        Text ageGroupText = new Text();
        ageGroupText.readFields(dataInput);
        ageGroup = ageGroupText.toString();
    }
}


