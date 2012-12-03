import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class StringProcessing {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		final String fileName = "recv.2012-11-28-05";
		final String wFileName = "recv.2012-11-28-05_ip2long.csv";		
		
		File file = new File(wFileName);
		if (file.exists()) {
			file.delete();
		}

		try {
			BufferedReader br = new BufferedReader(
					new InputStreamReader(new FileInputStream(fileName), "UTF-8"));
			BufferedWriter bw = new BufferedWriter(
					new OutputStreamWriter(new FileOutputStream(wFileName), "UTF-8"));
			
			String line = "";
			SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			while ( (line = br.readLine()) != null )
			{
				if (line.trim().equals("")) {
					continue;
				}
				
				String[] lineArrs = line.split("\\t"); 
				for (int i = 0; i < lineArrs.length; i++) {		//length=14
					lineArrs[i] = lineArrs[i].trim();
					if (i == 1 || i == 2) {						//the datetime column
						Date dt = format.parse(lineArrs[i]);
						bw.write("\"" + dt.getTime()/1000 +"\"" + ",");
					} else if (i == 3 || i == 5 || i == 7) {	//convert ip address to long
						String[] address = lineArrs[i].split("\\.");
						//2^24=16777216;2^16=65536;2^8=256
						long intOfip = Integer.parseInt(address[0])*16777216l + Integer.parseInt(address[1])*65536 +
								Integer.parseInt(address[2])*256 +Integer.parseInt(address[3]);
						bw.write("\"" + intOfip + "\"" + ",");
					} else {
						bw.write("\"" + lineArrs[i] + "\"" + ",");
					}
				}
				bw.write(",,,");
				bw.write("\n");
			}

			//System.out.println("while is over!");
			br.close();
			bw.flush();
			bw.close();
		} catch (IOException e) {
			System.out.println("IOException occur");
			e.getMessage();
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}

}
