import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/*
* This is a Mapper class which will map the records based on Age Group and Salaries of Employees
* Here we have input key class as LongWritable which will take input as Line no
* And input value class as Text which will take Line content
* Mapper class is going to generate output key of type EmpGrpKey(AgeGroup)
* and Output value of type DoubleWritable salary and it will write this data
*  record by record to MapReduce Framework using context.write
*
*
*
* */
public class SalaryMapper extends Mapper<LongWritable, Text, EmpGrpKey, DoubleWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context
    ) throws IOException, InterruptedException {

        StringTokenizer itr = new StringTokenizer(value.toString());
        String departmentName=itr.nextToken();
        String firstName = itr.nextToken();
        String lastName = itr.nextToken();
        int age = Integer.parseInt(itr.nextToken());
        double salary = Double.parseDouble(itr.nextToken());
        String ageGrp = null;
        if(age>=50 && age<=59)
        {
            ageGrp="Group 50-59";
        }
        if(age>=40 && age<=49)
        {
            ageGrp="Group 40-49";
        }
        if(age>=30 && age<=39)
        {
            ageGrp="Group 30-39";
        }
        if(age>=20 && age<=29)
        {
            ageGrp="Group 20-29";
        }
        //write data to MapReduce Framework
        context.write(new EmpGrpKey(ageGrp), new DoubleWritable(salary));
    }
}
