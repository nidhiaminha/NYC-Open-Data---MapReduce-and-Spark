import java.io.*;
import java.util.Scanner;

public class ritish
{
    public static void main(String args[]) throws IOException
    {
		String value;
		String opString = "";
		BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\nidhi\\Downloads\\input.txt"));
		while((value = br.readLine()) != null ) {
			String input = value.toString();
			String[] sampletokenSplit = input.split(",");
            String tempString="";
            for(int i=0;i<=sampletokenSplit.length; i++)
            {
			
                if (i==0 || i==2 ||i==3 || i==10|| i==11 || i==12 || i==13 || i==14 || i==15 || i== 16 || i== 17 || i==24 )
                {
                    if(sampletokenSplit[i].isEmpty())
                    {
						tempString = ""; 
                        break;
                    }
                    else
					{   
						tempString += sampletokenSplit[i] + " ";
                    }
                }			
            }
			//opString += tempString + "\n";
					System.out.println("--------------------");

			System.out.println(tempString);
					System.out.println("--------------------");

		}
		System.out.println("--------------------");
		System.out.println(opString);
	}
}