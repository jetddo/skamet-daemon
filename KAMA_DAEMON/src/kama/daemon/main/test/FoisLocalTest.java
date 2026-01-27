package kama.daemon.main.test;

import java.awt.Color;
import java.io.BufferedReader;
import java.io.FileReader;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

public class FoisLocalTest {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		double[] hpaList = new double[]{
				50
				,70
				,100
				,150
				,200
				,250
				,300
				,350
				,400
				,450
				,500
				,550
				,600
				,650
				,700
				,750
				,800
				,850
				,875
				,900
				,925
				,950
				,975
				,1000};
		
		for(int i=0 ; i<hpaList.length ; i++) {
			
			double hpa = hpaList[i];
			double feet = (1 -  Math.pow(hpa / 1013.25, 0.190284)) * 145366.45;
			
			System.out.println("hPa: "+hpa+", feet: "+feet);
			
		}
	}

	
	
	
}
