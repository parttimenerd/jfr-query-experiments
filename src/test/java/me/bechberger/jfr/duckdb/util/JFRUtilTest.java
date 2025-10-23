package me.bechberger.jfr.duckdb.util;

import static org.junit.jupiter.api.Assertions.*;

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

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
        assertEquals(expected, JFRUtil.decodeBytecodeClassName(input));
    }

    static Stream<Arguments> provideSimplifyBytecodeTests() {
        return Stream.of(
                Arguments.of(null, null),
                Arguments.of("", ""),
                Arguments.of("(L;)V", "(L;)V"),
                Arguments.of("(I)V", "(int)"),
                Arguments.of("(Ljava/lang/String;I)V", "(String, int)"),
                Arguments.of("([I[[Ljava/lang/Object;)V", "(int[], Object[][])"),
                Arguments.of("(D[FJ)V", "(double, float[], long)"),
                Arguments.of("()V", "()"),
                Arguments.of("(Ljava/util/List;[[[I)Ljava/lang/String;", "(List, int[][][])"));
    }

    @ParameterizedTest
    @MethodSource("provideSimplifyBytecodeTests")
    void simplifyMethodDescriptor(String input, String expected) {
        assertEquals(expected, JFRUtil.simplifyDescriptor(input));
    }

    static Stream<Arguments> provideCombineDescriptionsTests() {
        return Stream.of(
                Arguments.of("Bla", "This is a test.", "Additional info.", "This is a test. Additional info."),
                Arguments.of("BlaBla", "Single description.", null, null)
        );
    }

    @ParameterizedTest
    @MethodSource("provideCombineDescriptionsTests")
    void combineDescriptions(String entityName, String label, String description, String expected) {
        assertEquals(expected, JFRUtil.combineDescription(entityName, label, description));
    }
}