package com.orientechnologies.orient.core.collate;

import java.util.Comparator;

public class NaturalStringCollate implements OCollate{


    public static final String NAME = "NATURAL";

    private final Comparator<Object> naturalComparator = new NaturalStringComparator();

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Object transform(Object obj) {
        return obj;
    }

    @Override
    public int compareForOrderBy(Object o1, Object o2) {
        return naturalComparator.compare(o1, o2);
    }

    // 自然字符串比较器实现
    private static class NaturalStringComparator implements Comparator<Object> {
        @Override
        public int compare(Object o1, Object o2) {
            if (o1 == null && o2 == null) return 0;
            if (o1 == null) return -1;
            if (o2 == null) return 1;

            String s1 = o1.toString();
            String s2 = o2.toString();

            return naturalCompare(s1, s2);
        }

        private int naturalCompare(String a, String b) {
            int[] na = new int[1];
            int[] nb = new int[1];

            while (true) {
                // 跳过前导零
                while (isZero(a, na) && isZero(b, nb)) {
                    a = a.substring(na[0]);
                    b = b.substring(nb[0]);
                    na[0] = 0;
                    nb[0] = 0;
                }

                // 提取数字部分
                int diff = getNum(a, na) - getNum(b, nb);
                if (diff != 0) return diff;

                // 如果都到达末尾，则相等
                if (na[0] >= a.length() && nb[0] >= b.length()) return 0;

                // 处理边界情况
                if (na[0] >= a.length()) return -1;
                if (nb[0] >= b.length()) return 1;

                // 提取非数字部分
                diff = a.charAt(na[0]++) - b.charAt(nb[0]++);
                if (diff != 0) return diff;
            }
        }

        private boolean isZero(String s, int[] pos) {
            pos[0] = 0;
            while (pos[0] < s.length() && s.charAt(pos[0]) == '0') pos[0]++;
            return pos[0] < s.length() && Character.isDigit(s.charAt(pos[0]));
        }

        private int getNum(String s, int[] pos) {
            int start = pos[0];
            while (pos[0] < s.length() && Character.isDigit(s.charAt(pos[0]))) pos[0]++;
            return pos[0] == start ? 0 : Integer.parseInt(s.substring(start, pos[0]));
        }
    }

}
