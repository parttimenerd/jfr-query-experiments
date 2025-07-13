import me.bechberger.jfr.extended.Lexer;
import me.bechberger.jfr.extended.Parser;
import me.bechberger.jfr.extended.ast.ASTNodes.*;
import java.lang.reflect.Field;
import java.util.List;

/**
 * A utility class for testing the parser and visualizing the AST structure.
 * This class provides enhanced AST printing capabilities to display the parse tree
 * in a readable, hierarchical format with proper indentation and field details.
 */
public class TestParser {
    public static void main(String[] args) {
        // Use command line argument if provided, otherwise use a default query
        String query = args.length > 0 ? args[0] : "SELECT * FROM jdk.CPULoad WHERE value > 0.5";
        
        System.out.println("Query: " + query);
        System.out.println("=" + "=".repeat(query.length() + 6));
        
        try {
            Lexer lexer = new Lexer(query);
            Parser parser = new Parser(lexer.tokenize());
            ProgramNode ast = parser.parse();
            System.out.println("✓ Parsed successfully!");
            System.out.println("\nAST Structure:");
            System.out.println("─".repeat(50));
            printAST(ast, 0);
        } catch (Exception e) {
            System.out.println("✗ Exception: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * Prints the AST in a concise tree structure.
     */
    private static void printAST(Object node, int indent) {
        String prefix = "  ".repeat(indent);
        
        if (node == null) {
            System.out.println(prefix + "null");
            return;
        }
        
        String className = node.getClass().getSimpleName();
        
        // Handle primitive types and strings directly
        if (isPrimitive(node)) {
            System.out.println(prefix + node);
            return;
        }
        
        // Handle collections (List, Array, etc.)
        if (node instanceof List) {
            List<?> list = (List<?>) node;
            if (list.isEmpty()) {
                System.out.println(prefix + "[]");
            } else {
                System.out.println(prefix + "[" + list.size() + " items]");
                for (Object item : list) {
                    printAST(item, indent + 1);
                }
            }
            return;
        }
        
        // Print node type
        System.out.println(prefix + className);
        
        // For AST nodes, print important fields only
        try {
            Field[] fields = node.getClass().getDeclaredFields();
            for (Field field : fields) {
                field.setAccessible(true);
                Object value = field.get(node);
                
                if (value != null && !isBoringField(field.getName())) {
                    System.out.print(prefix + "  " + field.getName() + ": ");
                    if (isPrimitive(value)) {
                        System.out.println(value);
                    } else {
                        System.out.println();
                        printAST(value, indent + 2);
                    }
                }
            }
        } catch (IllegalAccessException e) {
            System.out.println(prefix + "  <error: " + e.getMessage() + ">");
        }
    }
    
    /**
     * Checks if a field should be skipped for brevity.
     */
    private static boolean isBoringField(String fieldName) {
        return fieldName.equals("location") || fieldName.equals("line") || fieldName.equals("column");
    }
    
    /**
     * Checks if an object is a primitive type or string that should be printed directly.
     * 
     * @param obj The object to check
     * @return true if the object is a primitive type or string
     */
    private static boolean isPrimitive(Object obj) {
        return obj instanceof String || 
               obj instanceof Number || 
               obj instanceof Boolean || 
               obj instanceof Character ||
               obj instanceof Enum ||
               obj.getClass().isPrimitive();
    }
}
