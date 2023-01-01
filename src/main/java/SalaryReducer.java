import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;

/*
 * This is a Reducer class which apply reducing logic on the salaries of employees
 * Here we have input key class as EmpGrpKey which is Custom Key Class
 * And input value class List of values of type DoubleWritable
 * Reducer class is going to generate output key of type Text which will represent AgeGroup
 * and Output value of Text which will be concatanated string of Avg min and max salaries
 *
 *
 *
 *
 * */

public class SalaryReducer extends Reducer<EmpGrpKey, DoubleWritable, EmpGrpKey, Text> {
    @Override
    public void reduce(EmpGrpKey key, Iterable<DoubleWritable> values,
                       Context context
    ) throws IOException, InterruptedException {
        //All values are intialized to 0 first
        Double sum = 0.0;
        int cnt = 0;
        Double avg = 0.0;
        Double min = 0.0;
        Double max = 0.0;
        int flag1=0;

        for (DoubleWritable val : values) {
            if(flag1==0)
            {
                //this will execute only once
                min=val.get();
                max=val.get();
                flag1=1;
            }
            sum += val.get();
            cnt += 1;
            if(val.get()<min)
            {
                min=val.get();
            }
            if(val.get()>max)
            {
                max=val.get();
            }
        }
        avg = sum / cnt;
        //After decimal points only 2 digits are allowed in Salary Output
        DecimalFormat df=new DecimalFormat("0.00");

        //nf.setMaximumFractionDigits(2);
      //  System.out.println("Reduce : Key " + key.toString() + " " + avg + " " + cnt + " " + sum);

        String salaryOutput="min="+df.format(min)+" "+"avg=" +df.format(avg)+ " "+"max="+df.format(max);
        Text sal=new Text(salaryOutput);
        context.write(key, sal);

    }
}

