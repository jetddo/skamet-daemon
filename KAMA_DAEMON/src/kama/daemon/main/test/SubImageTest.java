package kama.daemon.main.test;

import java.awt.image.BufferedImage;
import java.io.File;

import javax.imageio.ImageIO;

public class SubImageTest {

	public SubImageTest() throws Exception {
		
		int x = 5602/2; // 시작 x 좌표
		int y = 931/2; // 시작 y 좌표
		int width = 1138/2; // subImage 너비
		int height = 545/2; // subImage 높이
		
		BufferedImage gktgBaseImg = ImageIO.read(new File("F:/data/cdm_gktg_base.png"));
		
		BufferedImage bi = ImageIO.read(new File("F:/data/datastore/GKTG_IMG/2019/07/08/18/11.png"));
		
		bi = bi.getSubimage(x, y, width, height);
		
		gktgBaseImg.getGraphics().drawImage(bi, 0, 0, gktgBaseImg.getWidth(), gktgBaseImg.getHeight(), null);
		
		ImageIO.write(gktgBaseImg, "png", new File("F:/data/cdm_gktg_test.png"));
		
	}

	public static void main(String[] args) throws Exception {
		
		new SubImageTest();
	}

}
