using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Threading.Tasks;

namespace DocDb.DotNetSdk.Demos
{
	public static class Cleanup
	{
		public async static Task Run()
		{
			Console.WriteLine();
			Console.WriteLine(">>> Cleanup <<<");

			var endpoint = ConfigurationManager.AppSettings["DocDbEndpoint"];
			var masterKey = ConfigurationManager.AppSettings["DocDbMasterKey"];

			using (var client = new DocumentClient(new Uri(endpoint), masterKey))
			{
				// Delete documents created by demos
				Console.WriteLine("Deleting documents created by demos...");
				var sql = @"
				SELECT VALUE c._self FROM c
				 WHERE
					c.id IN('NEWDOC', '_metadata') OR
					c.name IN('John Doe') OR
					STARTSWITH(c.id, 'DUPEJ') = true OR
					STARTSWITH(c.id, 'MUFASA') = true OR
					STARTSWITH(c.name, 'New Customer') = true
					OR STARTSWITH(c.name, 'Bulk inserted doc ') = true
				";

				Database database = client.CreateDatabaseQuery("SELECT * FROM c WHERE c.id = 'mydb'").AsEnumerable().First();
				DocumentCollection collection = client.CreateDocumentCollectionQuery(database.SelfLink, "SELECT * FROM c WHERE c.id = 'mystore'").AsEnumerable().First();
				IEnumerable<StoredProcedure> sprocs = client.CreateStoredProcedureQuery(collection.StoredProceduresLink).AsEnumerable();

				if (sprocs.Any(sp => sp.Id == "spBulkDelete"))
				{
					var sprocLink = sprocs.First(sp => sp.Id == "spBulkDelete").SelfLink;
					await client.ExecuteStoredProcedureAsync<object>(sprocLink, sql);
				}
				else
				{
					var documentLinks = client.CreateDocumentQuery(collection.SelfLink, sql).AsEnumerable();
					foreach (string documentLink in documentLinks)
					{
						await client.DeleteDocumentAsync(documentLink);
					}
				}

				// Delete all stored procedures
				Console.WriteLine("Deleting all stored procedures...");
				foreach (var sproc in sprocs)
				{
					await client.DeleteStoredProcedureAsync(sproc.SelfLink);
				}

				// Delete all user defined functions
				Console.WriteLine("Deleting all user defined functions...");
				var udfs = client.CreateUserDefinedFunctionQuery(collection.UserDefinedFunctionsLink).AsEnumerable();
				foreach (var udf in udfs)
				{
					await client.DeleteUserDefinedFunctionAsync(udf.SelfLink);
				}

				// Delete all triggers
				Console.WriteLine("Deleting all triggers...");
				var triggers = client.CreateTriggerQuery(collection.UserDefinedFunctionsLink).AsEnumerable();
				foreach (var trigger in triggers)
				{
					await client.DeleteTriggerAsync(trigger.SelfLink);
				}

				// Delete all users
				Console.WriteLine("Deleting all users...");
				var users = client.CreateUserQuery(database.UsersLink).AsEnumerable();
				foreach (var user in users)
				{
					await client.DeleteUserAsync(user.SelfLink);
				}

			}

		}

	}
}
