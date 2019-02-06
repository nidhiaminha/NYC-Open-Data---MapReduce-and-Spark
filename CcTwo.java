
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Scanner;

public class CcTwo {

	public static void main(String[] args) throws IOException{

		Scanner in = new Scanner(new FileReader("part-r-00000"));      
		String Display_date = new String();
		int Compare_date = -1;
		
		String Display_borough  = new String();
		int Compare_borough = -1;
		
		String Display_zip  = new String();
		int Compare_zip = -1;
		
		String Display_vehicle = new String();
		int Compare_vehicle = -1;
		
		String Display_year_PPInjured = new String();
		int Compare_year_PPInjured = -1;
		
		String Display_year_PPKilled = new String();
		int Compare_year_PPKilled = -1;
		
		String Display_Cyclist = new String();
		int Compare_Cyclist = -1;
		
		String Display_Motorist = new String();
		int Compare_Motorist = -1;
		
		while(in.hasNext()) 
		{
			String line = in.nextLine();
			if(line.equals("")) continue;
			
			
			if(line.startsWith("Date_")) {
				String sub_line = line.substring(5);
				int index = sub_line.lastIndexOf((char)(9));
				//int index1 = sub_line.lastIndexOf(" ");
				//index = Math.max(index, index1);
				String first = sub_line.substring(0, index);
				int val = Integer.parseInt(sub_line.substring(index+1, sub_line.length()));
				if(val>Compare_date) {
					Compare_date = val;
					Display_date = first;
				}
			}
			else if(line.startsWith("B_Fatal")) {
				
				String sub_line = line.substring(7);
				//char c = (char)(9);
				int index = sub_line.lastIndexOf((char)(9));
				//int index1 = sub_line.lastIndexOf(" ");
				//index = Math.max(index, index1);
				String first = sub_line.substring(0, index);
				int val = Integer.parseInt(sub_line.substring(index+1, sub_line.length()));
				if(val>Compare_borough) {
					Compare_borough = val;
					Display_borough = first;
				}
			}
			else if(line.startsWith("Z_Fatal")) {
				
				String sub_line = line.substring(7);
				//char c = (char)(9);
				int index = sub_line.lastIndexOf((char)(9));
				//int index = temp.lastIndexOf(c);
				//int index1 = temp.lastIndexOf(" ");
				//index = Math.max(index, index);
				String first = sub_line.substring(0, index);
				int val = Integer.parseInt(sub_line.substring(index+1, sub_line.length()));
				if(val>Compare_zip) {
					Compare_zip = val;
					Display_zip = first;
				}
				
			}
			else if(line.startsWith("Veh_type")) {
				
				String sub_line = line.substring(8);
				//char c = (char)(9);
				int index = sub_line.lastIndexOf((char)(9));
				//int index = temp.lastIndexOf(c);
				//int index1 = temp.lastIndexOf(" ");
				//index = Math.max(index, index1);
				String first = sub_line.substring(0, index);
				int val = Integer.parseInt(sub_line.substring(index+1, sub_line.length()));
				if(val>Compare_vehicle) {
					Compare_vehicle = val;
					Display_vehicle = first;
				}
				
			}
			else if(line.startsWith("Num_pp_inj")) {
				
				String sub_line = line.substring(10);
				//char c = (char)(9);
				int index = sub_line.lastIndexOf((char)(9));
				//int index = temp.lastIndexOf(c);
				//int index1 = temp.lastIndexOf(" ");
				//index = Math.max(index, index1);
				String first = sub_line.substring(0, index);
				int val = Integer.parseInt(sub_line.substring(index+1, sub_line.length()));
				if(val>Compare_year_PPInjured) {
					Compare_year_PPInjured = val;
					Display_year_PPInjured = first;
				}
				
			}
			else if(line.startsWith("Num_pp_kil")) {
				
				String sub_line = line.substring(10);
				//char c = (char)(9);
				int index = sub_line.lastIndexOf((char)(9));
				//int index = temp.lastIndexOf(c);
				//int index1 = temp.lastIndexOf(" ");
				//index = Math.max(index, index1);
				String first = sub_line.substring(0, index);
				int val = Integer.parseInt(sub_line.substring(index+1, sub_line.length()));
				if(val>Compare_year_PPKilled) {
					Compare_year_PPKilled = val;
					Display_year_PPKilled = first;
				}
			}
			else if(line.startsWith("Num_c_ik")) {
				String sub_line = line.substring(8);
				//char c = (char)(9);
				int index = sub_line.lastIndexOf((char)(9));
				//int index = temp.lastIndexOf(c);
				//int index1 = temp.lastIndexOf(" ");
				//index = Math.max(index, index1);
				String first = sub_line.substring(0, index);
				int val = Integer.parseInt(sub_line.substring(index+1, sub_line.length()));
				if(val>Compare_Cyclist) {
					Compare_Cyclist = val;
					Display_Cyclist = first;
				}
				
			}
			else if(line.startsWith("Num_m_ikol")) {
				String sub_line = line.substring(10);
				//char c = (char)(9);
				int index = sub_line.lastIndexOf((char)(9));
				//int index = temp.lastIndexOf(c);
				//int index1 = temp.lastIndexOf(" ");
				//index = Math.max(index, index1);
				String first = sub_line.substring(0, index);
				int val = Integer.parseInt(sub_line.substring(index+1, sub_line.length()));
				if(val>Compare_Motorist) {
					Compare_Motorist = val;
					Display_Motorist = first;
				}
			}

		}
		System.out.println(Display_date + "  " + Compare_date);
		System.out.println(Display_borough + " " + Compare_borough);
		System.out.println(Display_zip + " " + Compare_zip);
		System.out.println(Display_vehicle + " " + Compare_vehicle);
		System.out.println(Display_year_PPInjured + " " + Compare_year_PPInjured);
		System.out.println(Display_year_PPKilled + " " + Compare_year_PPKilled);
		System.out.println(Display_Cyclist + " " + Compare_Cyclist);
		System.out.println(Display_Motorist + " " + Compare_Motorist);
		
		
	}

}
