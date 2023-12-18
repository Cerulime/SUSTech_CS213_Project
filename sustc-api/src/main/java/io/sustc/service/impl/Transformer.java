package io.sustc.service.impl;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@Setter
@Getter
public class Transformer {
    private static final int[] bvState = {11, 10, 3, 8, 4, 6};
    private static final long bvXOR = 177451812L;
    private static final long bvAdd = 8728348608L;
    @SuppressWarnings("SpellCheckingInspection")
    private static final char[] trTable = "fZodR9XQDSUm21yCkr6zBqiveYah8bt4xsWpHnJE7jL5VG3guMTKNPAwcF".toCharArray();
    private static final int[] tr = new int[128];
    private static final long[] pow58 = new long[6];

    static {
        for (int i = 0; i < 58; i++) {
            tr[trTable[i]] = i;
        }
        pow58[0] = 1;
        for (int i = 1; i < 6; i++) {
            pow58[i] = pow58[i - 1] * 58;
        }
    }

    private long avCount;

    public Transformer() {
        this.avCount = 10001;
    }

    public long getAv(String bv) {
        long r = 0;
        for (int i = 0; i < 6; i++) {
            r += pow58[i] * tr[bv.charAt(bvState[i])];
        }
        return (r - bvAdd) ^ bvXOR;
    }

    public String getBv(long av) {
        av = (av ^ bvXOR) + bvAdd;
        char[] r = "BV1  4 1 7  ".toCharArray();
        for (int i = 0; i < 6; i++) {
            r[bvState[i]] = trTable[(int) (av / pow58[i] % 58)];
        }
        return new String(r);
    }

    public String generateBV() {
        avCount++;
        return getBv(avCount);
    }
}
