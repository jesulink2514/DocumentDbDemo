using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace DocDb.DotNetSdk.Demos
{
	public static class UserDefinedFunctionsDemo
	{
		private static DocumentCollection _collection;

		public async static Task Run()
		{
			Debugger.Break();

			var endpoint = ConfigurationManager.AppSettings["DocDbEndpoint"];
			var masterKey = ConfigurationManager.AppSettings["DocDbMasterKey"];

			using (var client = new DocumentClient(new Uri(endpoint), masterKey))
			{
				Database database = client.CreateDatabaseQuery("SELECT * FROM c WHERE c.id = 'mydb'").AsEnumerable().First();
				_collection = client.CreateDocumentCollectionQuery(database.CollectionsLink, "SELECT * FROM c WHERE c.id = 'mystore'").AsEnumerable().First();

				await CreateUserDefinedFunctions(client);

				ViewUserDefinedFunctions(client);

				Execute_udfRegEx(client);
				Execute_udfIsNorthAmerica(client);
				Execute_udfFormatCityStateZip(client);

				await DeleteUserDefinedFunctions(client);
			}
		}

		private async static Task CreateUserDefinedFunctions(DocumentClient client)
		{
			Console.WriteLine();
			Console.WriteLine(">>> Create User Defined Functions <<<");
			Console.WriteLine();

			await CreateUserDefinedFunction(client, "udfRegEx");
			await CreateUserDefinedFunction(client, "udfIsNorthAmerica");
			await CreateUserDefinedFunction(client, "udfFormatCityStateZip");
		}

		private async static Task<UserDefinedFunction> CreateUserDefinedFunction(DocumentClient client, string udfId)
		{
			var udfBody = File.ReadAllText(@"..\..\Server\" + udfId + ".js");
			var udfDefinition = new UserDefinedFunction
			{
				Id = udfId,
				Body = udfBody
			};

			var result = await client.CreateUserDefinedFunctionAsync(_collection.SelfLink, udfDefinition);
			var udf = result.Resource;
			Console.WriteLine("Created user defined function {0}; RID: {1}", udf.Id, udf.ResourceId);

			return udf;
		}

		private static void ViewUserDefinedFunctions(DocumentClient client)
		{
			Console.WriteLine();
			Console.WriteLine(">>> View UDFs <<<");
			Console.WriteLine();

			var udfs = client
				.CreateUserDefinedFunctionQuery(_collection.UserDefinedFunctionsLink)
				.ToList();

			foreach (var udf in udfs)
			{
				Console.WriteLine("User defined function {0}; RID: {1}", udf.Id, udf.ResourceId);
			}
		}

		private static void Execute_udfRegEx(DocumentClient client)
		{
			var sql = "SELECT c.name FROM c WHERE udf.udfRegEx(c.name, 'Rental') != null";

			Console.WriteLine();
			Console.WriteLine("Querying for Rental customers");
			var documents = client.CreateDocumentQuery(_collection.SelfLink, sql).ToList();

			Console.WriteLine("Found {0} Rental customers:", documents.Count);
			foreach (var document in documents)
			{
				Console.WriteLine(" {0}", document.name);
			}
		}

		private static void Execute_udfIsNorthAmerica(DocumentClient client)
		{
			var sql = "SELECT c.name, c.address.countryRegionName FROM c WHERE udf.udfIsNorthAmerica(c.address.countryRegionName) = true";

			Console.WriteLine();
			Console.WriteLine("Querying for North American customers");
			var documents = client.CreateDocumentQuery(_collection.SelfLink, sql).ToList();

			Console.WriteLine("Found {0} North American customers; first 20:", documents.Count);
			foreach (var document in documents.Take(20))
			{
				Console.WriteLine(" {0}, {1}", document.name, document.countryRegionName);
			}

			sql = "SELECT c.name, c.address.countryRegionName FROM c WHERE udf.udfIsNorthAmerica(c.address.countryRegionName) = false";

			Console.WriteLine();
			Console.WriteLine("Querying for non North American customers");
			documents = client.CreateDocumentQuery(_collection.SelfLink, sql).ToList();

			Console.WriteLine("Found {0} non North American customers; first 20", documents.Count);
			foreach (var document in documents.Take(20))
			{
				Console.WriteLine(" {0}, {1}", document.name, document.countryRegionName);
			}
		}

		private static void Execute_udfFormatCityStateZip(DocumentClient client)
		{
			var sql = "SELECT c.name, udf.udfFormatCityStateZip(c) AS csz FROM c";

			Console.WriteLine();
			Console.WriteLine("Listing names with city, state, zip (first 20)");

			var documents = client.CreateDocumentQuery(_collection.SelfLink, sql).ToList();
			foreach (var document in documents.Take(20))
			{
				Console.WriteLine(" {0} located in {1}", document.name, document.csz);
			}
		}

		private async static Task DeleteUserDefinedFunctions(DocumentClient client)
		{
			Console.WriteLine();
			Console.WriteLine(">>> Delete User Defined Functions <<<");
			Console.WriteLine();

			await DeleteUserDefinedFunction(client, "udfRegEx");
			await DeleteUserDefinedFunction(client, "udfIsNorthAmerica");
			await DeleteUserDefinedFunction(client, "udfFormatCityStateZip");
		}

		private async static Task DeleteUserDefinedFunction(DocumentClient client, string udfId)
		{
			var udf = client
				.CreateUserDefinedFunctionQuery(_collection.UserDefinedFunctionsLink)
				.AsEnumerable()
				.First(u => u.Id == udfId);

			await client.DeleteUserDefinedFunctionAsync(udf.SelfLink);

			Console.WriteLine("Deleted user defined function: {0}", udfId);
		}

	}
}
