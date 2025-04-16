#!/usr/bin/env node
import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { CallToolRequestSchema, ListResourcesRequestSchema, ListToolsRequestSchema, ReadResourceRequestSchema, } from "@modelcontextprotocol/sdk/types.js";
import pg from "pg";
import fs from "fs/promises";
import path from "path";
// Create the server.
const server = new Server({
    name: "example-servers/postgres",
    version: "0.1.0",
}, {
    capabilities: {
        resources: {},
        tools: {},
    },
});
const args = process.argv.slice(2);
if (args.length === 0) {
    console.error("Please provide a database URL as a command-line argument");
    process.exit(1);
}
const databaseUrl = args[0];
const resourceBaseUrl = new URL(databaseUrl);
resourceBaseUrl.protocol = "postgres:";
resourceBaseUrl.password = "";
const pool = new pg.Pool({
    connectionString: databaseUrl,
});
// Constants for resource paths
const SCHEMA_PATH = "schema";
const DATA_PATH = "data";
const QUERY_PATH = "query";
// Helper: A generic query executor that enforces types.
// The change is here: constrain T to extend pg.QueryResultRow.
async function executeQuery(sql, params = []) {
    const client = await pool.connect();
    try {
        const result = await client.query(sql, params);
        return result;
    }
    finally {
        client.release();
    }
}
// Helper: Get columns of a table.
async function getTableColumns(tableName) {
    const result = await executeQuery("SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = $1", [tableName]);
    return result.rows;
}
// Helper: Get primary keys of a table.
async function getTablePrimaryKeys(tableName) {
    const result = await executeQuery(`
    SELECT a.attname
    FROM pg_index i
    JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
    WHERE i.indrelid = $1::regclass AND i.indisprimary
    `, [`public.${tableName}`]);
    return result.rows.map((row) => row.attname);
}
// Helper: Get foreign keys of a table.
async function getTableForeignKeys(tableName) {
    const result = await executeQuery(`
    SELECT
      kcu.column_name,
      ccu.table_name AS foreign_table_name,
      ccu.column_name AS foreign_column_name
    FROM
      information_schema.table_constraints AS tc
      JOIN information_schema.key_column_usage AS kcu
        ON tc.constraint_name = kcu.constraint_name
        AND tc.table_schema = kcu.table_schema
      JOIN information_schema.constraint_column_usage AS ccu
        ON ccu.constraint_name = tc.constraint_name
        AND ccu.table_schema = tc.table_schema
    WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_name = $1
    `, [tableName]);
    return result.rows;
}
// Helper: Get table size.
async function getTableSize(tableName) {
    const result = await executeQuery("SELECT pg_size_pretty(pg_total_relation_size($1)) as size", [`public.${tableName}`]);
    return result.rows[0].size;
}
// Helper: Get database statistics.
async function getDatabaseStats() {
    const result = await executeQuery(`
    SELECT 
      datname AS database_name,
      pg_size_pretty(pg_database_size(datname)) AS size,
      pg_size_pretty(pg_database_size(datname) - 
                    COALESCE((SELECT sum(pg_relation_size(pg_class.oid))
                              FROM pg_class
                              LEFT JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace
                              WHERE pg_namespace.nspname = 'pg_toast'), 0)) AS data_size
    FROM pg_database
    WHERE datname = current_database()
  `);
    return result.rows[0];
}
// List Resources Handler
server.setRequestHandler(ListResourcesRequestSchema, async () => {
    const result = await executeQuery("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'");
    const resources = [];
    // For every table, add schema and data resource links.
    for (const row of result.rows) {
        resources.push({
            uri: new URL(`${row.table_name}/${SCHEMA_PATH}`, resourceBaseUrl).href,
            mimeType: "application/json",
            name: `"${row.table_name}" database schema`,
        });
        resources.push({
            uri: new URL(`${row.table_name}/${DATA_PATH}`, resourceBaseUrl).href,
            mimeType: "application/json",
            name: `"${row.table_name}" table data`,
        });
    }
    // Add database statistics resource.
    resources.push({
        uri: new URL("stats", resourceBaseUrl).href,
        mimeType: "application/json",
        name: "Database statistics",
    });
    return { resources };
});
// Read Resource Handler
server.setRequestHandler(ReadResourceRequestSchema, async (request) => {
    const resourceUrl = new URL(request.params.uri);
    // Remove empty parts (leading/trailing slashes)
    const pathComponents = resourceUrl.pathname.split("/").filter((p) => p !== "");
    // If the URI is like postgres://host/stats, return the stats.
    if (pathComponents.length === 1 && pathComponents[0] === "stats") {
        const stats = await getDatabaseStats();
        const tablesResult = await executeQuery("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'");
        const tables = tablesResult.rows;
        const tableDetails = [];
        for (const table of tables) {
            const size = await getTableSize(table.table_name);
            const countResult = await executeQuery(`SELECT COUNT(*) as count FROM "${table.table_name}"`);
            const rowCount = countResult.rows[0].count;
            tableDetails.push({
                name: table.table_name,
                size,
                rowCount,
            });
        }
        const statsWithTables = { ...stats, tables: tableDetails };
        return {
            contents: [
                {
                    uri: request.params.uri,
                    mimeType: "application/json",
                    text: JSON.stringify(statsWithTables, null, 2),
                },
            ],
        };
    }
    // Expect table resources to have path form: /{tableName}/{resourceType}
    if (pathComponents.length === 2) {
        const [tableName, resourceType] = pathComponents;
        // Schema resource.
        if (resourceType === SCHEMA_PATH) {
            const columns = await getTableColumns(tableName);
            const primaryKeys = await getTablePrimaryKeys(tableName);
            const foreignKeys = await getTableForeignKeys(tableName);
            const tableSize = await getTableSize(tableName);
            const countResult = await executeQuery(`SELECT COUNT(*) as count FROM "${tableName}"`);
            const rowCount = countResult.rows[0].count;
            const schemaInfo = { tableName, columns, primaryKeys, foreignKeys, tableSize, rowCount };
            return {
                contents: [
                    {
                        uri: request.params.uri,
                        mimeType: "application/json",
                        text: JSON.stringify(schemaInfo, null, 2),
                    },
                ],
            };
        }
        // Data resource.
        if (resourceType === DATA_PATH) {
            const dataResult = await executeQuery(`SELECT * FROM "${tableName}" LIMIT 1000`);
            return {
                contents: [
                    {
                        uri: request.params.uri,
                        mimeType: "application/json",
                        text: JSON.stringify(dataResult.rows, null, 2),
                    },
                ],
            };
        }
    }
    throw new Error("Invalid resource URI");
});
// List Tools Handler
server.setRequestHandler(ListToolsRequestSchema, async () => {
    return {
        tools: [
            {
                name: "query",
                description: "Run a read-only SQL query",
                inputSchema: {
                    type: "object",
                    properties: {
                        sql: { type: "string" },
                    },
                },
            },
            {
                name: "describe_table",
                description: "Get detailed information about a table structure",
                inputSchema: {
                    type: "object",
                    properties: {
                        table_name: { type: "string" },
                    },
                },
            },
            {
                name: "count_rows",
                description: "Count rows in a table optionally with a WHERE condition",
                inputSchema: {
                    type: "object",
                    properties: {
                        table_name: { type: "string" },
                        condition: { type: "string", optional: true },
                    },
                },
            },
            {
                name: "find_relationships",
                description: "Find direct relationships between two tables",
                inputSchema: {
                    type: "object",
                    properties: {
                        table1: { type: "string" },
                        table2: { type: "string" },
                    },
                },
            },
            {
                name: "analyze_query",
                description: "Analyze the execution plan of a SQL query",
                inputSchema: {
                    type: "object",
                    properties: {
                        sql: { type: "string" },
                    },
                },
            },
            {
                name: "export_data",
                description: "Export table data to a JSON file",
                inputSchema: {
                    type: "object",
                    properties: {
                        table_name: { type: "string" },
                        output_path: { type: "string" },
                        limit: { type: "number", optional: true },
                        where_clause: { type: "string", optional: true },
                    },
                },
            },
        ],
    };
});
// Call Tool Handler
server.setRequestHandler(CallToolRequestSchema, async (request) => {
    const toolName = request.params.name;
    const args = request.params.arguments || {};
    if (toolName === "query") {
        const sql = args.sql;
        if (!sql) {
            throw new Error("SQL query is required");
        }
        const client = await pool.connect();
        try {
            await client.query("BEGIN TRANSACTION READ ONLY");
            const result = await client.query(sql);
            return {
                content: [{ type: "text", text: JSON.stringify(result.rows, null, 2) }],
                isError: false,
            };
        }
        catch (error) {
            throw error;
        }
        finally {
            try {
                await client.query("ROLLBACK");
            }
            catch (err) {
                console.warn("Could not roll back transaction:", err);
            }
            client.release();
        }
    }
    else if (toolName === "describe_table") {
        const tableName = args.table_name;
        if (!tableName) {
            throw new Error("Table name is required");
        }
        try {
            const columns = await getTableColumns(tableName);
            const primaryKeys = await getTablePrimaryKeys(tableName);
            const foreignKeys = await getTableForeignKeys(tableName);
            const tableSize = await getTableSize(tableName);
            const countResult = await executeQuery(`SELECT COUNT(*) as count FROM "${tableName}"`);
            const rowCount = countResult.rows[0].count;
            const sampleData = (await executeQuery(`SELECT * FROM "${tableName}" LIMIT 5`))
                .rows;
            const indices = (await executeQuery(`
            SELECT
              i.relname as index_name,
              a.attname as column_name,
              ix.indisunique as is_unique
            FROM
              pg_class t,
              pg_class i,
              pg_index ix,
              pg_attribute a
            WHERE
              t.oid = ix.indrelid
              AND i.oid = ix.indexrelid
              AND a.attrelid = t.oid
              AND a.attnum = ANY(ix.indkey)
              AND t.relkind = 'r'
              AND t.relname = $1
            ORDER BY
              t.relname,
              i.relname
            `, [tableName])).rows;
            const tableInfo = {
                tableName,
                rowCount,
                tableSize,
                columns,
                primaryKeys,
                foreignKeys,
                indices,
                sampleData,
            };
            return {
                content: [{ type: "text", text: JSON.stringify(tableInfo, null, 2) }],
                isError: false,
            };
        }
        catch (error) {
            throw error;
        }
    }
    else if (toolName === "count_rows") {
        const tableName = args.table_name;
        const condition = args.condition;
        if (!tableName) {
            throw new Error("Table name is required");
        }
        try {
            let sql = `SELECT COUNT(*) as count FROM "${tableName}"`;
            if (condition) {
                sql += ` WHERE ${condition}`;
            }
            const result = await executeQuery(sql);
            return {
                content: [
                    {
                        type: "text",
                        text: JSON.stringify({
                            table: tableName,
                            count: parseInt(result.rows[0].count, 10),
                            condition: condition || null,
                        }, null, 2),
                    },
                ],
                isError: false,
            };
        }
        catch (error) {
            throw error;
        }
    }
    else if (toolName === "find_relationships") {
        const table1 = args.table1;
        const table2 = args.table2;
        if (!table1 || !table2) {
            throw new Error("Both table names are required");
        }
        try {
            // Direct relationships from table1 to table2.
            const fkTable1ToTable2 = await executeQuery(`
          SELECT
            kcu.column_name as from_column,
            ccu.column_name as to_column
          FROM
            information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
              ON tc.constraint_name = kcu.constraint_name
              AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage AS ccu
              ON ccu.constraint_name = tc.constraint_name
              AND ccu.table_schema = tc.table_schema
          WHERE tc.constraint_type = 'FOREIGN KEY' 
            AND tc.table_name = $1
            AND ccu.table_name = $2
          `, [table1, table2]);
            // Direct relationships from table2 to table1.
            const fkTable2ToTable1 = await executeQuery(`
          SELECT
            kcu.column_name as from_column,
            ccu.column_name as to_column
          FROM
            information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
              ON tc.constraint_name = kcu.constraint_name
              AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage AS ccu
              ON ccu.constraint_name = tc.constraint_name
              AND ccu.table_schema = tc.table_schema
          WHERE tc.constraint_type = 'FOREIGN KEY' 
            AND tc.table_name = $1
            AND ccu.table_name = $2
          `, [table2, table1]);
            // All foreign key relationships.
            const allFKRelationships = await executeQuery(`
          SELECT
            tc.table_name as from_table,
            kcu.column_name as from_column,
            ccu.table_name as to_table,
            ccu.column_name as to_column
          FROM
            information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
              ON tc.constraint_name = kcu.constraint_name
              AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage AS ccu
              ON ccu.constraint_name = tc.constraint_name
              AND ccu.table_schema = tc.table_schema
          WHERE tc.constraint_type = 'FOREIGN KEY'
        `);
            const intermediateRelationships = [];
            const fromTable1 = allFKRelationships.rows.filter((r) => r.from_table === table1);
            for (const rel1 of fromTable1) {
                for (const rel2 of allFKRelationships.rows) {
                    if (rel1.to_table === rel2.from_table && rel2.to_table === table2) {
                        intermediateRelationships.push({
                            path: `${table1} -> ${rel1.to_table} -> ${table2}`,
                            details: {
                                step1: {
                                    from_table: table1,
                                    from_column: rel1.from_column,
                                    to_table: rel1.to_table,
                                    to_column: rel1.to_column,
                                },
                                step2: {
                                    from_table: rel2.from_table,
                                    from_column: rel2.from_column,
                                    to_table: table2,
                                    to_column: rel2.to_column,
                                },
                            },
                        });
                    }
                }
            }
            const relationships = {
                direct: {
                    [`${table1} -> ${table2}`]: fkTable1ToTable2.rows.map((r) => ({
                        from_column: r.from_column,
                        to_column: r.to_column,
                    })),
                    [`${table2} -> ${table1}`]: fkTable2ToTable1.rows.map((r) => ({
                        from_column: r.from_column,
                        to_column: r.to_column,
                    })),
                },
                indirect: intermediateRelationships,
            };
            return {
                content: [{ type: "text", text: JSON.stringify(relationships, null, 2) }],
                isError: false,
            };
        }
        catch (error) {
            throw error;
        }
    }
    else if (toolName === "analyze_query") {
        const sql = args.sql;
        if (!sql) {
            throw new Error("SQL query is required");
        }
        try {
            const result = await executeQuery(`EXPLAIN (FORMAT JSON, ANALYZE, VERBOSE) ${sql}`);
            // Type-cast to allow access to the "QUERY PLAN" property.
            const queryPlan = result.rows[0]["QUERY PLAN"][0];
            return {
                content: [{ type: "text", text: JSON.stringify(queryPlan, null, 2) }],
                isError: false,
            };
        }
        catch (error) {
            throw error;
        }
    }
    else if (toolName === "export_data") {
        const tableName = args.table_name;
        const outputPath = args.output_path;
        const limit = args.limit || 10000;
        const whereClause = args.where_clause || "";
        if (!tableName || !outputPath) {
            throw new Error("Table name and output path are required");
        }
        try {
            let sql = `SELECT * FROM "${tableName}"`;
            if (whereClause) {
                sql += ` WHERE ${whereClause}`;
            }
            sql += ` LIMIT ${limit}`;
            const result = await executeQuery(sql);
            // Ensure directory exists.
            const directory = path.dirname(outputPath);
            await fs.mkdir(directory, { recursive: true });
            // Write data to file.
            await fs.writeFile(outputPath, JSON.stringify(result.rows, null, 2));
            return {
                content: [
                    {
                        type: "text",
                        text: JSON.stringify({
                            table: tableName,
                            rows_exported: result.rows.length,
                            output_file: outputPath,
                        }, null, 2),
                    },
                ],
                isError: false,
            };
        }
        catch (error) {
            throw error;
        }
    }
    throw new Error(`Unknown tool: ${toolName}`);
});
async function runServer() {
    const transport = new StdioServerTransport();
    await server.connect(transport);
}
runServer().catch(console.error);
