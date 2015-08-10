package cn.adsage.dc.dataPercent;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Test {

	@org.junit.Test
	public void test() {
		// String str =
		// "20150720004850  00      8497    24097   46000   285554  15770   3437b019418f42b6a462fb99f2e22a77                70:DE:E2:A0:83:7C   999000000        999000000       iPad2,1 ipad    ios     1       1       1       49F839DF52CB7CF36022    239D90337A2667FDEFE9    20150720004849       1.161.193.61    0 0                     Gigu!/1.2 CFNetwork/672.1.15 Darwin/14.0.0      Gigu!   MobiSageSDK         undefined        15502   728     90      383     13              4       7.1.2   3       2       0.39    2.3.0   1       1       0   999000000        20150720";
		String str = "27164   a108b0b1e4d44fa49550d75af72609cb        139520000       iPad3,4         6.0.1   1       1";
		String[] split = str.replaceAll("\\s+", ";").split(";");
		System.out.println(split.length);
		System.out.println(split[3]);

		String num = "16";
		System.out.println(Test.isNumeric(num));
		
		String a = "iPad3,1;1";
		String[] split2 = a.split(";");
		System.out.println(split2.length);
		
		

	}

	public static boolean isNumeric(String str) {
		Pattern pattern = Pattern.compile("[0-9]*");
		Matcher isNum = pattern.matcher(str);
		if (!isNum.matches()) {
			return false;
		}
		return true;
	}

}
