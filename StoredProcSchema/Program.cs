//-----------------------------------------------------------------------
// <copyright file="Program.cs" company="Richard Smith">
//     Copyright (c) Richard Smith. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

namespace StoredProcSchema
{
    using System;
    using System.Data;
    using System.Data.SqlClient;
    using System.Linq;
    using System.Reflection;

    using McMaster.Extensions.CommandLineUtils;

    /// <summary>
    /// StoredProcSchema's console application.
    /// </summary>
    internal class Program
    {
        /// <summary>
        /// Entry point for StoredProcSchema's console application.
        /// </summary>
        /// <param name="args">Command line parameters.</param>
        private static void Main(string[] args)
        {
            var app = new CommandLineApplication();
            app.Name = "StoredProcSchema";
            app.Description = "Console app to report the schema of a stored proc result";

            app.HelpOption();
            app.ThrowOnUnexpectedArgument = false;
            app.MakeSuggestionsInErrorMessage = true;
            app.VersionOptionFromAssemblyAttributes(Assembly.GetExecutingAssembly());

            var connectionString = app.Option("-c|--connection-string", "Connection string to use", CommandOptionType.SingleValue).IsRequired();
            var storedProc = app.Option("-s|--stored-proc", "Stored proc to execute", CommandOptionType.SingleValue).IsRequired();

            var parameters = app.Option("-p|--param", "Stored proc parameters", CommandOptionType.MultipleValue);

            var outputScript = app.Option("-os|--output-script", "Generate table, table type & insert proc scripts as output", CommandOptionType.NoValue);
            var outputSchema = app.Option("-sc|--output-schema", "Output schema name to use.", CommandOptionType.SingleValue);
            var outputName = app.Option("-n|--output-name", "Output name to use.", CommandOptionType.SingleValue);

            app.OnExecute(async () =>
            {
                using (var db = new SqlConnection(connectionString.Value()))
                {
                    await db.OpenAsync();

                    using (var cmd = new SqlCommand(storedProc.Value(), db) { CommandType = CommandType.StoredProcedure })
                    {
                        foreach (var parameter in parameters.Values)
                        {
                            var val = parameter.Split('|');
                            cmd.Parameters.AddWithValue(val[0], val[1]);
                        }

                        using (var result = await cmd.ExecuteReaderAsync())
                        {
                            var dt = result.GetSchemaTable();

                            if (outputScript.HasValue())
                            {
                                Console.WriteLine($"DROP PROCEDURE IF EXISTS [{outputSchema.Value()}].[Load{outputName.Value()}]");
                                Console.WriteLine($"DROP TABLE IF EXISTS [{outputSchema.Value()}].[{outputName.Value()}]");
                                Console.WriteLine($"DROP TYPE IF EXISTS [{outputSchema.Value()}].[{outputName.Value()}Type]");
                                Console.WriteLine();

                                Console.WriteLine($"CREATE TABLE [{outputSchema.Value()}].[{outputName.Value()}] (");

                                foreach (DataRow row in dt.Rows)
                                {
                                    var scaleInfo = row["DataType"].Equals(typeof(string)) ? $"({(row["ColumnSize"].Equals(int.MaxValue) ? "max" : row["ColumnSize"])})" : string.Empty;
                                    Console.WriteLine($"    [{row["ColumnName"]}] [{row["DataTypeName"]}]{scaleInfo} {(row["AllowDBNull"].Equals(true) ? string.Empty : "NOT ")}NULL,");
                                }

                                Console.WriteLine($"    [DateInserted] [datetime] NOT NULL");
                                Console.WriteLine($")");

                                Console.WriteLine();

                                Console.WriteLine($"CREATE TYPE [{outputSchema.Value()}].[{outputName.Value()}Type] AS TABLE (");

                                bool first = true;
                                foreach (DataRow row in dt.Rows)
                                {
                                    if (!first)
                                    {
                                        Console.WriteLine(",");
                                    }

                                    first = false;

                                    var scaleInfo = row["DataType"].Equals(typeof(string)) ? $"({(row["ColumnSize"].Equals(int.MaxValue) ? "max" : row["ColumnSize"])})" : string.Empty;
                                    Console.Write($"    [{row["ColumnName"]}] [{row["DataTypeName"]}]{scaleInfo} {(row["AllowDBNull"].Equals(true) ? string.Empty : "NOT ")}NULL");
                                }

                                Console.WriteLine();
                                Console.WriteLine($")");

                                Console.WriteLine();
                                Console.WriteLine(
@"SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO
");
                                Console.WriteLine($"CREATE PROCEDURE [{outputSchema.Value()}].[Load{outputName.Value()}]");
                                Console.WriteLine($"@Data [{outputSchema.Value()}].[{outputName.Value()}Type] READONLY");

                                Console.WriteLine(
@"AS
BEGIN");

                                //Console.WriteLine($"    TRUNCATE TABLE [{outputSchema.Value()}].[{outputName.Value()}]");
                                Console.WriteLine($"    INSERT INTO [{outputSchema.Value()}].[{outputName.Value()}] (");

                                Console.Write(string.Join("," + Environment.NewLine, dt.Rows.OfType<DataRow>().Select(x => $"        [{x["ColumnName"]}]")));
                                Console.WriteLine(",");
                                Console.WriteLine("        [DateInserted]");

                                Console.WriteLine($"    )");

                                Console.WriteLine($"    SELECT");

                                Console.Write(string.Join("," + Environment.NewLine, dt.Rows.OfType<DataRow>().Select(x => $"        [{x["ColumnName"]}]")));
                                Console.WriteLine(",");
                                Console.WriteLine("        GETDATE()");

                                Console.WriteLine($"    FROM @Data;");

                                Console.WriteLine($"END;");
                            }
                            else
                            {
                                Console.WriteLine(string.Join("|", dt.Columns.OfType<DataColumn>().Select(x => x.ColumnName)));
                                Console.WriteLine(string.Join(Environment.NewLine, dt.Rows.OfType<DataRow>().Select(x => string.Join("|", x.ItemArray))));
                            }
                        }
                    }
                }
            });

            app.Execute(args);
        }
    }
}
