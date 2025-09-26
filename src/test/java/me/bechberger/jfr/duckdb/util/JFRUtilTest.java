package me.bechberger.jfr.duckdb.util;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.*;

class JFRUtilTest {

    @ParameterizedTest
    @CsvSource({
            "Ljava/lang/String;,java.lang.String",
            "[I,int[]",
            "[[D,double[][]",
            "[[Ljava/util/List;,java.util.List[][]",
            "V,void",
            "I,int",
            "Z,boolean",
            "B,byte",
            "C,char",
            "S,short",
            "J,long",
            "F,float",
            "D,double",
            "[[[[Ljava/lang/Object;,java.lang.Object[][][][]",
            "[[[I,int[][][]"
    })
    void decodeBytecodeClassName(String input, String expected) {
        JFRUtil util = new JFRUtil();
        assertEquals(expected, util.decodeBytecodeClassName(input));
    }
}