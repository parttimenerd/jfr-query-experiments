package me.bechberger.jfr.extended.engine.framework;

import me.bechberger.jfr.extended.engine.framework.QueryTestFramework;
import me.bechberger.jfr.extended.engine.framework.QueryResult;

public class DebugNestedQueryTest {
    public static void main(String[] args) {
        QueryTestFramework framework = new QueryTestFramework();
        
        // Create the same test data
        framework.mockTable("Employees", """
            name | department | salary
            Alice | Sales | 60000
            Bob | Sales | 70000
            Charlie | IT | 80000
            Diana | IT | 90000
            """);
            
        framework.mockTable("Departments", """
            dept_name | budget
            Sales | 500000
            IT | 800000
            """);

        // Execute the query and see what we get
        String query = """
            @SELECT 
                name,
                department,
                salary,
                (@SELECT AVG(salary) FROM Employees e2 WHERE e2.department = e1.department) as avg_dept_salary
            FROM Employees e1
            ORDER BY department, name
            """;
            
        System.out.println("Executing query:");
        System.out.println(query);
        
        QueryResult result = framework.executeQuery(query);
        if (result.isSuccess()) {
            System.out.println("\nActual result:");
            var table = result.getTable();
            System.out.println("Rows: " + table.getRowCount());
            for (int i = 0; i < table.getRowCount(); i++) {
                System.out.println("Row " + i + ": " + 
                    table.getString(i, "name") + " | " + 
                    table.getString(i, "department") + " | " + 
                    table.getNumber(i, "salary") + " | " + 
                    table.getString(i, "avg_dept_salary")); // Use getString to see what's actually there
            }
        } else {
            System.out.println("Query failed: " + result.getError().getMessage());
        }
        
        // Now test the original JOIN subquery issue
        System.out.println("\n" + "=".repeat(60));
        System.out.println("Testing JOIN with subqueries:");
        
        framework.mockTable("Employees2", """
            emp_id | name | department | manager_id
            1 | Alice | Sales | 3
            2 | Bob | Sales | 3
            3 | Charlie | Sales | null
            4 | Diana | IT | null
            """);
            
        String joinQuery = """
            @SELECT
                e.name as employee_name,
                e.department,
                m.name as manager_name
            FROM (
                @SELECT emp_id, name, department, manager_id
                FROM Employees2
                WHERE department = 'Sales'
            ) as e
            LEFT JOIN (
                @SELECT emp_id, name
                FROM Employees2
                WHERE manager_id IS NULL
            ) as m ON e.manager_id = m.emp_id
            ORDER BY employee_name
            """;
            
        System.out.println("Executing JOIN query:");
        System.out.println(joinQuery);
        
        QueryResult joinResult = framework.executeQuery(joinQuery);
        if (joinResult.isSuccess()) {
            System.out.println("\nJOIN result:");
            var joinTable = joinResult.getTable();
            System.out.println("Rows: " + joinTable.getRowCount());
            for (int i = 0; i < joinTable.getRowCount(); i++) {
                System.out.println("Row " + i + ": " + 
                    joinTable.getString(i, "employee_name") + " | " + 
                    joinTable.getString(i, "department") + " | " + 
                    joinTable.getString(i, "manager_name"));
            }
        } else {
            System.out.println("JOIN query failed: " + joinResult.getError().getMessage());
        }
    }
}
