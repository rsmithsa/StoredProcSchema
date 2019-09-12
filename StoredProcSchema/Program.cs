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

                            Console.WriteLine(string.Join("|", dt.Columns.OfType<DataColumn>().Select(x => x.ColumnName)));
                            Console.WriteLine(string.Join(Environment.NewLine, dt.Rows.OfType<DataRow>().Select(x => string.Join("|", x.ItemArray))));
                        }
                    }
                }
            });

            app.Execute(args);
        }
    }
}
