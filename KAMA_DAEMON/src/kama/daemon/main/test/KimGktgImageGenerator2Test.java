package kama.daemon.main.test;

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.File;
import java.util.ArrayList;
import java.util.List;

import javax.imageio.ImageIO;

import kama.daemon.common.util.model.BoundLonLat;
import kama.daemon.common.util.model.BoundXY;
import kama.daemon.common.util.model.GridCalcUtil;
import kama.daemon.common.util.model.ModelGridUtil;
import kama.daemon.common.util.model.legendfilter.KimGktgLegendFilter;
import net.coobird.thumbnailator.Thumbnails;
import ucar.ma2.Range;
import ucar.nc2.Variable;
import ucar.nc2.dataset.NetcdfDataset;

public class KimGktgImageGenerator2Test {

    private ModelGridUtil modelGridUtil;

    private final int imageExpandFactor = 10;
    private final int imageResizeFactor = 1;

    private String varName = "GTGMAX";

    public KimGktgImageGenerator2Test() {
        this.initCoordinates();
    }

    private void initCoordinates() {

        System.out.println("KimGdpsImageGenerator [ Initailize Coordinate Systems ]");

        String coordinatesLatPath = "F:/data/datastore/grid/kim_gktg_lat.bin";
        String coordinatesLonPath = "F:/data/datastore/grid/kim_gktg_lon.bin";

        this.modelGridUtil = new ModelGridUtil(
            ModelGridUtil.Model.KIM_GKTG,
            ModelGridUtil.Position.MIDDLE_CENTER,
            coordinatesLatPath,
            coordinatesLonPath
        );
    }

    public void generateImages(NetcdfDataset ncFile, String fileName, String savePath) {

        System.out.println("KimGdpsImageGenerator [ Start Create Tile Images ]");
        System.out.println("\t-> Create KimGdps Image Grid List");

        double[] mapBound = new double[] { 80, -80, 0, 360 };

        this.modelGridUtil.setMultipleGridBoundInfoforLatLonGrid(mapBound);

        System.out.println(this.modelGridUtil.getBoundXY());

        System.out.println(
            "\t-> Create Image Bound ["
                + mapBound[0] + ", "
                + mapBound[1] + ", "
                + mapBound[2] + ", "
                + mapBound[3] + "]"
        );

        this.generateImage(ncFile, fileName, savePath);

        System.out.println("KimGdpsImageGenerator [ End Create Images ]");
    }

    public BoundLonLat generateImage(NetcdfDataset ncFile, String fileName, String savePath) {

        BoundLonLat boundLonLat = null;
        BoundXY boundXY = null;
        Graphics2D ig2 = null;

        try {
            boundLonLat = this.modelGridUtil.getBoundLonLat();
            boundXY = this.modelGridUtil.getBoundXY();

            System.out.println(boundLonLat);

            KimGktgLegendFilter kimGktgLegendFilter = new KimGktgLegendFilter();

            int imgHeight = (int) Math.floor(
                (boundLonLat.getTop() - boundLonLat.getBottom())
                    * this.imageExpandFactor
                    * this.imageResizeFactor
            );

            int imgWidth = (int) Math.floor(
                (boundLonLat.getRight() - boundLonLat.getLeft())
                    * this.imageExpandFactor
                    * this.imageResizeFactor
            );

            System.out.println("\t-> " + boundLonLat + ", " + boundXY);

            int rows = this.modelGridUtil.getRows();
            int cols = this.modelGridUtil.getCols();

            double[] latInterval = GridCalcUtil.calculateCumulativeArr(this.modelGridUtil.getLatInterval());
            double[] lonInterval = GridCalcUtil.calculateCumulativeArr(this.modelGridUtil.getLonInterval());

            double[] mercatorRatio = GridCalcUtil.calculateCumulativeArr(
                GridCalcUtil.getLatitudeRatioList(
                    boundLonLat.getTop(),
                    boundLonLat.getBottom(),
                    imgHeight,
                    imgHeight
                )
            );

            System.out.println("\t-> Process Attribute [" + this.varName + "]");

            Variable var = ncFile.findVariable(this.varName);
            if (var == null) {
                throw new IllegalArgumentException("Variable not found: " + this.varName);
            }

            // 픽셀 경계 미리 계산
            int[] xPixel = new int[cols + 1];
            for (int l = 0; l <= cols; l++) {
                xPixel[l] = (int) Math.floor(
                    lonInterval[l] * this.imageExpandFactor * this.imageResizeFactor
                );
            }

            int[] yPixel = new int[rows + 1];
            for (int k = 0; k <= rows; k++) {
                double yCoord = boundLonLat.getTop() - latInterval[k];

                int idx = (int) Math.floor(
                    (yCoord - boundLonLat.getBottom())
                        * this.imageExpandFactor
                        * this.imageResizeFactor
                );

                if (idx < 0) {
                    idx = 0;
                } else if (idx >= mercatorRatio.length) {
                    idx = mercatorRatio.length - 1;
                }

                yPixel[k] = (int) Math.floor(mercatorRatio[idx]);
            }

            for (int j = 0; j < 1; j++) {

                System.out.println("\t\t-> Start Read Variable [" + this.varName + "]");

                List<Range> rangeList = new ArrayList<Range>();
                rangeList.add(new Range(j, j));
                rangeList.add(new Range(
                    modelGridUtil.getModelHeight() - boundXY.getTop() - 1,
                    modelGridUtil.getModelHeight() - boundXY.getBottom() - 1
                ));
                rangeList.add(new Range(boundXY.getLeft(), boundXY.getRight()));

                long t0 = System.currentTimeMillis();

                Float[][] values = GridCalcUtil.convertStorageToValues(
                    var.read(rangeList).getStorage(),
                    rows,
                    cols
                );

                long t1 = System.currentTimeMillis();

                System.out.println("\t\t-> End Read Variable [" + this.varName + "]");

                String imgFileName = fileName.replace(".nc", "") + "_" + String.format("%03d", j + 1) + ".png";
                File imageFile = new File(savePath + File.separator + imgFileName);

                BufferedImage bi = new BufferedImage(imgWidth, imgHeight, BufferedImage.TYPE_INT_ARGB);
                ig2 = bi.createGraphics();

                System.out.println("\t\t-> Start Write Image [" + imageFile.getAbsolutePath() + "]");

                for (int k = 0; k < rows; k++) {

                    int yTop = yPixel[k];
                    int yBottom = yPixel[k + 1];
                    int rectY = Math.min(yTop, yBottom);
                    int rectHeight = Math.abs(yBottom - yTop);

                    if (rectHeight <= 0) {
                        continue;
                    }

                    int l = 0;
                    while (l < cols) {

                        Float valueObj = values[k][l];
                        if (valueObj == null) {
                            l++;
                            continue;
                        }

                        float v = valueObj.floatValue();
                        Color color = kimGktgLegendFilter.getColor_GTGMAX(v);

                        if (color == null) {
                            l++;
                            continue;
                        }

                        int start = l;
                        l++;

                        while (l < cols) {
                            Float nextValueObj = values[k][l];
                            if (nextValueObj == null) {
                                break;
                            }

                            Color nextColor = kimGktgLegendFilter.getColor_GTGMAX(nextValueObj.floatValue());

                            if (nextColor == null || !nextColor.equals(color)) {
                                break;
                            }

                            l++;
                        }

                        int xLeft = xPixel[start];
                        int xRight = xPixel[l];
                        int rectX = Math.min(xLeft, xRight);
                        int rectWidth = Math.abs(xRight - xLeft);

                        if (rectWidth <= 0) {
                            continue;
                        }

                        ig2.setColor(color);
                        ig2.fillRect(rectX, rectY, rectWidth, rectHeight);
                    }
                }

                long t2 = System.currentTimeMillis();

                ig2.dispose();
                ig2 = null;

                if (this.imageResizeFactor > 1) {
                    bi = Thumbnails.of(bi)
                        .imageType(BufferedImage.TYPE_INT_ARGB)
                        .size(
                            imgWidth / this.imageResizeFactor,
                            imgHeight / this.imageResizeFactor
                        )
                        .asBufferedImage();
                }

                ImageIO.write(bi, "PNG", imageFile);

                long t3 = System.currentTimeMillis();

                System.out.println("\t\t-> End Write Image [" + imageFile.getAbsolutePath() + "]");
                System.out.println("\t\t-> Read Time   : " + (t1 - t0) + " ms");
                System.out.println("\t\t-> Render Time : " + (t2 - t1) + " ms");
                System.out.println("\t\t-> Write Time  : " + (t3 - t2) + " ms");
                System.out.println("\t\t-> Total Time  : " + (t3 - t0) + " ms");
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (ig2 != null) {
                ig2.dispose();
            }
        }

        return boundLonLat;
    }

    public static void main(String[] args) {

    	KimGktgImageGenerator2Test kimGktgImageGeneratorTest = new KimGktgImageGenerator2Test();

        NetcdfDataset ncFile = null;

        try {
            ncFile = NetcdfDataset.acquireDataset(
                "F:/data/ftp/amo_kimg_gktg_max_f00_2026030418.nc",
                null
            );

            String fileName = "amo_kimg_gktg_max_f00_2026030418.nc";
            String savePath = "F:/data";

            long start = System.currentTimeMillis();

            kimGktgImageGeneratorTest.generateImages(ncFile, fileName, savePath);

            long end = System.currentTimeMillis();
            System.out.println("Elapsed Time : " + (end - start) + " ms");

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (ncFile != null) {
                    ncFile.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}