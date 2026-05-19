package kama.daemon.main.test;

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.FontMetrics;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.geom.Ellipse2D;
import java.awt.geom.Path2D;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.SwingUtilities;

public class TideCurveSample extends JPanel {

    static class TidePoint {
        double xRate;
        double tide;
        boolean low;

        public TidePoint(double xRate, double tide, boolean low) {
            this.xRate = xRate;
            this.tide = tide;
            this.low = low;
        }
    }

    private final List<TidePoint> points = Arrays.asList(
            new TidePoint(0.10, 120, true),
            new TidePoint(0.35, 680, false),
            new TidePoint(0.60, 180, true),
            new TidePoint(0.85, 720, false)
    );

    private final double minTide = 0;
    private final double maxTide = 800;

    private final int padding = 60;
    private final int iconRadius = 16;

    public TideCurveSample() {
        setPreferredSize(new Dimension(900, 420));
        setBackground(Color.WHITE);
    }

    @Override
    protected void paintComponent(Graphics g) {
        super.paintComponent(g);

        Graphics2D g2 = (Graphics2D) g.create();

        g2.setRenderingHint(
                RenderingHints.KEY_ANTIALIASING,
                RenderingHints.VALUE_ANTIALIAS_ON
        );

        drawChartBox(g2);
        drawTideCurve(g2);
        drawTideIcons(g2);

        g2.dispose();
    }

    private void drawTideCurve(Graphics2D g2) {

        List<TidePoint> curvePoints = new ArrayList<TidePoint>();

        TidePoint first = points.get(0);
        TidePoint last = points.get(points.size() - 1);

        // 왼쪽 화면 밖 가상 반대 조위
        curvePoints.add(new TidePoint(
                -0.15,
                first.tide,
                !first.low
        ));

        curvePoints.addAll(points);

        // 오른쪽 화면 밖 가상 반대 조위
        curvePoints.add(new TidePoint(
                1.15,
                last.tide,
                !last.low
        ));

        Path2D path = new Path2D.Double();

        for (int i = 0; i < curvePoints.size() - 1; i++) {

            TidePoint current = curvePoints.get(i);
            TidePoint next = curvePoints.get(i + 1);

            double x1 = toScreenX(current.xRate);
            double y1 = toCurveY(current);

            double x2 = toScreenX(next.xRate);
            double y2 = toCurveY(next);

            int steps = 80;

            for (int s = 0; s <= steps; s++) {

                double t = (double) s / steps;

                double x = lerp(x1, x2, t);
                double y = cosineInterpolate(y1, y2, t);

                // 화면 안쪽만 그림
                if (x < padding || x > getWidth() - padding) {
                    continue;
                }

                if (path.getCurrentPoint() == null) {
                    path.moveTo(x, y);
                } else {
                    path.lineTo(x, y);
                }
            }
        }

        g2.setStroke(new BasicStroke(3f));
        g2.setColor(new Color(30, 120, 220));
        g2.draw(path);
    }

    private void drawTideIcons(Graphics2D g2) {

        for (TidePoint p : points) {

            double x = toScreenX(p.xRate);
            double y = toScreenY(p.tide);

            Ellipse2D circle = new Ellipse2D.Double(
                    x - iconRadius,
                    y - iconRadius,
                    iconRadius * 2,
                    iconRadius * 2
            );

            g2.setColor(Color.WHITE);
            g2.fill(circle);

            if (p.low) {
                g2.setColor(new Color(40, 130, 220));
            } else {
                g2.setColor(new Color(220, 80, 60));
            }

            g2.setStroke(new BasicStroke(2f));
            g2.draw(circle);

            g2.setFont(new Font("Dialog", Font.BOLD, 15));

            String text;

            if (p.low) {
                text = "저";
            } else {
                text = "고";
            }

            FontMetrics fm = g2.getFontMetrics();

            int tx = (int) (x - fm.stringWidth(text) / 2.0);
            int ty = (int) (y + fm.getAscent() / 2.0 - 2);

            g2.drawString(text, tx, ty);
        }
    }

    /**
     * 커브가 실제 아이콘을 피해서 지나가는 좌표
     */
    private double toCurveY(TidePoint p) {

        double y = toScreenY(p.tide);

        if (p.low) {
            // 저점은 아래쪽으로 우회
            return y + iconRadius + 4;
        } else {
            // 고점은 위쪽으로 우회
            return y - iconRadius - 4;
        }
    }

    private double toScreenX(double xRate) {

        int chartWidth = getWidth() - padding * 2;

        return padding + chartWidth * xRate;
    }

    private double toScreenY(double tide) {

        int chartHeight = getHeight() - padding * 2;

        double rate = (tide - minTide) / (maxTide - minTide);

        return getHeight() - padding - chartHeight * rate;
    }

    private void drawChartBox(Graphics2D g2) {

        g2.setColor(new Color(220, 220, 220));

        g2.drawRect(
                padding,
                padding,
                getWidth() - padding * 2,
                getHeight() - padding * 2
        );
    }

    /**
     * 부드러운 곡선 보간
     */
    private static double cosineInterpolate(double start, double end, double t) {

        double mu = (1 - Math.cos(t * Math.PI)) / 2.0;

        return start * (1 - mu) + end * mu;
    }

    private static double lerp(double start, double end, double t) {

        return start + (end - start) * t;
    }

    public static void main(String[] args) {

        SwingUtilities.invokeLater(new Runnable() {

            @Override
            public void run() {

                JFrame frame = new JFrame("Tide Curve Sample");

                frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

                frame.setContentPane(new TideCurveSample());

                frame.pack();

                frame.setLocationRelativeTo(null);

                frame.setVisible(true);
            }
        });
    }
}