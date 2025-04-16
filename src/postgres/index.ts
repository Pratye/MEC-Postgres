#!/usr/bin/env node

import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import {
  CallToolRequestSchema,
  ListResourcesRequestSchema,
  ListToolsRequestSchema,
  ReadResourceRequestSchema,
} from "@modelcontextprotocol/sdk/types.js";
import pg from "pg";
import fs from "fs/promises";
import { z } from "zod";
import { parse } from "csv-parse/sync";

// Define a Zod schema for the CSV upload request.
const UploadCsvRequestSchema = z.object({
  method: z.literal("UploadCsvRequest"),
  params: z.object({
    fileName: z.string(),
    fileData: z.string(), // base64 encoded string of the CSV file's data
  }),
});

const server = new Server(
  {
    name: "example-servers/postgres",
    version: "0.1.0",
  },
  {
    capabilities: {
      resources: {},
      tools: {},
    },
  },
);

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

const SCHEMA_PATH = "schema";

server.setRequestHandler(ListResourcesRequestSchema, async () => {
  const client = await pool.connect();
  try {
    const result = await client.query<{ table_name: string }>(
      "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
    );
    return {
      resources: result.rows.map((row) => ({
        uri: new URL(`${row.table_name}/${SCHEMA_PATH}`, resourceBaseUrl).href,
        mimeType: "application/json",
        name: `"${row.table_name}" database schema`,
      })),
    };
  } finally {
    client.release();
  }
});

server.setRequestHandler(ReadResourceRequestSchema, async (request) => {
  const resourceUrl = new URL(request.params.uri);

  const pathComponents = resourceUrl.pathname.split("/");
  const schema = pathComponents.pop();
  const tableName = pathComponents.pop();

  if (schema !== SCHEMA_PATH) {
    throw new Error("Invalid resource URI");
  }

  const client = await pool.connect();
  try {
    const result = await client.query<{ column_name: string; data_type: string }>(
      "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = $1",
      [tableName]
    );

    return {
      contents: [
        {
          uri: request.params.uri,
          mimeType: "application/json",
          text: JSON.stringify(result.rows, null, 2),
        },
      ],
    };
  } finally {
    client.release();
  }
});

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
          required: ["sql"],
        },
      },
      {
        name: "uploadCsv",
        description: "Upload and process a CSV file into database tables",
        inputSchema: {
          type: "object",
          properties: {
            fileName: { type: "string" },
            fileData: { type: "string", description: "Base64 encoded CSV file data" },
            tableName: { type: "string", description: "Name for the database table to be created (optional)" },
          },
          required: ["fileName", "fileData"],
        },
      },
    ],
  };
});

server.setRequestHandler(CallToolRequestSchema, async (request: { params: { name: string; arguments?: any } }) => {
  try {
    console.log(`Handling tool request: ${request.params.name}`);
    
    // Log arguments in a safe way to prevent JSON parsing errors
    if (request.params.arguments) {
      console.log(`Arguments type:`, typeof request.params.arguments);
      try {
        // If arguments is a string, try to parse it as JSON
        if (typeof request.params.arguments === 'string') {
          request.params.arguments = JSON.parse(request.params.arguments);
          console.log('Parsed string arguments to object');
        }
      } catch (e) {
        console.log(`Could not parse arguments as JSON: ${e}`);
      }
    }
  if (request.params.name === "query") {
    let sql;
    try {
      // Handle the case where arguments might be a string
      if (typeof request.params.arguments === 'string') {
        try {
          const parsedArgs = JSON.parse(request.params.arguments);
          sql = parsedArgs.sql;
        } catch (e) {
          // If it's not parseable JSON but starts with SELECT, it might be a direct SQL query
          if (request.params.arguments.trim().toUpperCase().startsWith('SELECT')) {
            sql = request.params.arguments;
          } else {
            throw new Error(`Invalid SQL query format: ${e}`);
          }
        }
      } else {
        sql = request.params.arguments?.sql;
      }
      
      if (!sql) {
        throw new Error("No SQL query provided");
      }
      
      console.log(`Executing SQL query: ${sql}`);
      const client = await pool.connect();
      try {
        await client.query("BEGIN TRANSACTION READ ONLY");
        const result = await client.query<any>(sql);
        return {
          content: [{ type: "text", text: JSON.stringify(result.rows, null, 2) }],
          isError: false,
        };
      } catch (error) {
        console.error(`SQL query error: ${error}`);
        return {
          content: [{ type: "text", text: `Error executing SQL query: ${error}` }],
          isError: true,
        };
      } finally {
        client
          .query("ROLLBACK")
          .catch((error) =>
            console.warn("Could not roll back transaction:", error)
          );
        client.release();
      }
    } catch (error) {
      console.error(`Query preparation error: ${error}`);
      return {
        content: [{ type: "text", text: `Error preparing SQL query: ${error}` }],
        isError: true,
      };
    }
  } else if (request.params.name === "uploadCsv") {
    try {
      let fileName, fileData, tableName;
      
      // Handle the case where arguments might be a string
      if (typeof request.params.arguments === 'string') {
        try {
          const parsedArgs = JSON.parse(request.params.arguments);
          fileName = parsedArgs.fileName;
          fileData = parsedArgs.fileData;
          tableName = parsedArgs.tableName;
        } catch (e) {
          throw new Error(`Could not parse CSV upload arguments: ${e}`);
        }
      } else {
        ({ fileName, fileData, tableName } = request.params.arguments);
      }
      
      if (!fileName || !fileData) {
        throw new Error("Missing required parameters: fileName and fileData");
      }
      
      console.log(`Processing CSV file: ${fileName}`);
      const fileBuffer = Buffer.from(fileData, "base64");
      console.log(`File data decoded, length: ${fileBuffer.length} bytes`);
      
      const derivedTableName = tableName || getTableNameFromFileName(fileName);
      console.log(`Using table name: ${derivedTableName}`);
      
      const result = await processCsvUpload(fileBuffer, derivedTableName);
      console.log(`CSV processing complete: ${JSON.stringify(result)}`);
      
      return {
        content: [{ type: "text", text: JSON.stringify(result, null, 2) }],
        isError: false,
      };
    } catch (error) {
      console.error(`Error processing CSV:`, error);
      return {
        content: [{ type: "text", text: `Error processing CSV: ${error}` }],
        isError: true,
      };
    }
  } else {
    console.warn(`Unknown tool: ${request.params.name}`);
    return {
      content: [{ type: "text", text: `Unknown tool: ${request.params.name}` }],
      isError: true,
    };
  }
} catch (error) {
    console.error(`Error handling tool request: ${error}`);
    return {
      content: [{ type: "text", text: `Error handling tool request: ${error}` }],
      isError: true,
    };
  }
});

// -----------------------------------------------------------------------------
// Helper function to extract a valid table name from file name
// -----------------------------------------------------------------------------
function getTableNameFromFileName(fileName: string): string {
  // Extract base name without extension
  const baseName = fileName.split('.')[0];
  // Replace spaces and special characters with underscores
  return baseName.replace(/[^a-zA-Z0-9]/g, '_').toLowerCase();
}

// -----------------------------------------------------------------------------
// Function to process CSV file uploads
// -----------------------------------------------------------------------------
interface TableSummary {
  created: number;
  updated: number;
  skipped: number;
}

async function processCsvUpload(
  fileBuffer: Buffer,
  tableName: string
): Promise<{ [tableName: string]: TableSummary }> {
  const csvString = fileBuffer.toString('utf-8');
  const client = await pool.connect();
  const summary: { [tableName: string]: TableSummary } = {};
  
  try {
    // Parse the CSV content
    const records = parse(csvString, {
      columns: true,
      skip_empty_lines: true,
      trim: true
    });
    
    if (!records.length) {
      return { [tableName]: { created: 0, updated: 0, skipped: 0 } };
    }
    
    summary[tableName] = { created: 0, updated: 0, skipped: 0 };
    
    await client.query("BEGIN");
    
    // Check if table exists - using a more compatible approach
    let tableExists = false;
    try {
      // Try to query the table
      const testQuery = `SELECT 1 FROM "${tableName}" LIMIT 1`;
      await client.query(testQuery);
      tableExists = true;
    } catch (err) {
      // Table doesn't exist if we get an error
      tableExists = false;
    }
    
    // We've already determined tableExists using the try-catch approach above
    
    if (!tableExists) {
      // Create table using the headers from the first row
      const columns = Object.keys(records[0]);
      const columnDefinitions = columns
        .map((col) => {
          if (col.toLowerCase() === "id") {
            return `"${col}" TEXT PRIMARY KEY`;
          }
          return `"${col}" TEXT`;
        })
        .join(", ");
      
      const createTableSQL = `CREATE TABLE "${tableName}" (${columnDefinitions});`;
      await client.query(createTableSQL);
    }
    
    // Process each row in the CSV
    for (const row of records) {
      let existsInDb = false;
      
      // If the row has an 'id', use it as the unique identifier
      if (row.hasOwnProperty("id") && row.id !== null && row.id !== '') {
        const selectQuery = `SELECT * FROM "${tableName}" WHERE "id" = $1`;
        const selectResult = await client.query<any>(selectQuery, [row.id]);
        if (selectResult.rows.length > 0) {
          existsInDb = true;
        }
      } else {
        // Without an 'id', compare all column values for a match
        const cols = Object.keys(row);
        const whereClause = cols
          .map((col, index) => `"${col}" = ${index + 1}`)
          .join(" AND ");
        
        const selectQuery = `SELECT * FROM "${tableName}" WHERE ${whereClause}`;
        const values = Object.values(row);
        const selectResult = await client.query<any>(selectQuery, values);
        
        if (selectResult.rows.length > 0) {
          existsInDb = true;
        }
      }
      
      if (existsInDb) {
        // Update the existing record if 'id' is available; otherwise, skip updating
        if (row.hasOwnProperty("id") && row.id !== null && row.id !== '') {
          const columnsToUpdate = Object.keys(row).filter((col) => col !== "id");
          
          if (columnsToUpdate.length > 0) {
            const setClause = columnsToUpdate
              .map((col, index) => `"${col}" = ${index + 2}`)
              .join(", ");
            
            const updateValues = [row.id, ...columnsToUpdate.map((col) => row[col])];
            const updateQuery = `UPDATE "${tableName}" SET ${setClause} WHERE "id" = $1`;
            
            await client.query(updateQuery, updateValues);
            summary[tableName].updated += 1;
          }
        } else {
          summary[tableName].skipped += 1;
        }
      } else {
        // Insert the new record
        const cols = Object.keys(row);
        const colNames = cols.map((col) => `"${col}"`).join(", ");
        const placeholders = cols.map((_, index) => `${index + 1}`).join(", ");
        
        const insertQuery = `INSERT INTO "${tableName}" (${colNames}) VALUES (${placeholders})`;
        await client.query(insertQuery, Object.values(row));
        
        summary[tableName].created += 1;
      }
    }
    
    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
  
  return summary;
}

// This handler is no longer needed since we're using the CallToolRequestSchema handler
// for the uploadCsv tool
// server.setRequestHandler(UploadCsvRequestSchema, async (request) => {
//   const { fileName, fileData } = request.params;
//   const fileBuffer = Buffer.from(fileData, "base64");
//   const result = await processCsvUpload(fileBuffer, getTableNameFromFileName(fileName));
//   return { status: "success", details: result };
// });

import { RestServerTransport } from "@chatmcp/sdk/server/rest.js";
import { getParamValue, getAuthValue } from "@chatmcp/sdk/utils/index.js";
 
const perplexityApiKey = getParamValue("perplexity_api_key") || "";
 
const mode = getParamValue("mode") || "stdio";
const port = getParamValue("port") || 9593;
const endpoint = getParamValue("endpoint") || "/rest";

async function runServer() {
  try {
    // after: MCP Server run with rest transport and stdio transport
    if (mode === "rest") {
      const transport = new RestServerTransport({
        port,
        endpoint,
      });
      await server.connect(transport);
 
      await transport.startServer();
 
      return;
    }
 
    // before: MCP Server only run with stdio transport
    const transport = new StdioServerTransport();
    await server.connect(transport);
    console.error(
      "Perplexity MCP Server running on stdio with Ask, Research, and Reason tools"
    );
  } catch (error) {
    console.error("Fatal error running server:", error);
    process.exit(1);
  }
}

runServer().catch(console.error);