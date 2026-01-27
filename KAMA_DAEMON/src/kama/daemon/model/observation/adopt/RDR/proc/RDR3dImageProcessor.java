package kama.daemon.model.observation.adopt.RDR.proc;

import kama.daemon.model.observation.adopt.RDR.data.Layer2D;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;

/**
 * Created by chlee on 2017-02-23.
 */
public class RDR3dImageProcessor
{
    private static Color[] m_color_table = null;

    /**
     * byte 값 범위별 레이더 색깔 정의 (한번만 수행).
     */
    private static void _initializeRadarColors()
    {
        if (m_color_table == null)
        {
            m_color_table = new Color[23];

            m_color_table[0] = new Color(110, 190, 255);
            m_color_table[1] = new Color(62, 148, 255);
            m_color_table[2] = new Color(62, 116, 255);
            m_color_table[3] = new Color(40, 49, 255);
            m_color_table[4] = new Color(70, 255, 70);
            m_color_table[5] = new Color(0, 224, 80);
            m_color_table[6] = new Color(0, 175, 101);
            m_color_table[7] = new Color(0, 150, 101);
            m_color_table[8] = new Color(255, 229, 0);
            m_color_table[9] = new Color(255, 190, 0);
            m_color_table[10] = new Color(255, 112, 0);
            m_color_table[11] = new Color(215, 0, 0);
            m_color_table[12] = new Color(255, 206, 255);
            m_color_table[13] = new Color(255, 170, 255);
            m_color_table[14] = new Color(255, 125, 250);
            m_color_table[15] = new Color(255, 0, 255); // 문의 필요. 엣지에 진한 핑크? 색이 나오는 결과가 나타남. 샘플이미지에는 없는 현상.
            //m_color_table[15] = new Color(0, 0, 0, 0); // 엣지 핑크 삭제하고 투명색으로 대체
            m_color_table[16] = new Color(136, 136, 136);
            m_color_table[17] = new Color(163, 163, 163);
            m_color_table[18] = new Color(110, 190, 255);
            //m_color_table[19] = new Color(224, 224, 224); //no echo
            m_color_table[19] = new Color(224, 224, 224, 0); //no echo
            m_color_table[20] = new Color(196, 196, 196); //... ..
            m_color_table[21] = new Color(255, 255, 255);
            m_color_table[22] = new Color(0, 0, 0);
        }
    }

    /**
     * 값에 따라 레이더 색깔 받아옴.
     * @param val
     * @return
     */
    public static Color getRadarColor(int val)
    {
        // 레이더 색깔 정의
        _initializeRadarColors();

        int cidx = -1;

        if (val >= 1 && val < 12) cidx = 0;
        else if (val >= 12 && val <= 18) cidx = 1;
        else if (val >= 19 && val <= 23) cidx = 2;
        else if (val >= 24 && val <= 28) cidx = 3;
        else if (val >= 29 && val <= 34) cidx = 4;
        else if (val >= 35 && val <= 39) cidx = 5;
        else if (val >= 40 && val <= 42) cidx = 6;
        else if (val >= 43 && val <= 44) cidx = 7;
        else if (val == 45) cidx = 8;
        else if (val >= 46 && val <= 47) cidx = 9;
        else if (val >= 48 && val <= 49) cidx = 10;
        else if (val == 50) cidx = 11;
        else if (val >= 51 && val <= 54) cidx = 12;
        else if (val == 55) cidx = 13;
        else if (val >= 56 && val <= 58) cidx = 14;
        else if (val >= 59 && val <= 99) cidx = 15;
        else if (val == -127) cidx = 19;
        else cidx = 19;

        if (cidx < 0 || m_color_table.length <= cidx)
        {
            return new Color(0, 0, 0, 0);
        }

        return m_color_table[cidx];
    }

    public static void createBitmapFromLayer(Layer2D layer2D, File outputFile, boolean flipVertical) throws IOException
    {
        BufferedImage img;
        img = createBitmapFromLayer(layer2D, flipVertical);

        ImageIO.write(img, "png", outputFile);
    }

    /**
     * 레이더 정보가 포함된 평면 오브젝트를 비트맵으로 변환해주는 함수
     * @param layer2D 평면 오브젝트
     * @return 이미지
     */
    public static BufferedImage createBitmapFromLayer(Layer2D layer2D, boolean flipVertical)
    {
        int width, height;

        width = layer2D.getSizeX();
        height = layer2D.getSizeY();

        BufferedImage bmp = new BufferedImage(width, height, BufferedImage.TYPE_4BYTE_ABGR);

        for (int y = 0; y < height; y++)
        {
            for (int x = 0; x < width; x++)
            {
                Color color = RDR3dImageProcessor.getRadarColor(Layer2D.stou(layer2D.getByteValue(x, y)));

                if (flipVertical)
                {
                    bmp.setRGB(x, height - y - 1, color.getRGB()); // 뒤집혀 있음
                }
                else
                {
                    bmp.setRGB(x, y, color.getRGB());
                }
            }
        }

        return bmp;
    }
}
