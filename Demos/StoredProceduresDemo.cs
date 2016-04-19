using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Script.Serialization;

namespace DocDb.DotNetSdk.Demos
{
	public static class StoredProceduresDemo
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

				await CreateStoredProcedures(client);

				ViewStoredProcedures(client);

				await ExecuteStoredProcedures(client);

				await DeleteStoredProcedures(client);
			}
		}

		// Create stored procedures

		private async static Task CreateStoredProcedures(DocumentClient client)
		{
			Console.WriteLine();
			Console.WriteLine(">>> Create Stored Procedures <<<");
			Console.WriteLine();

			await CreateStoredProcedure(client, "spHelloWorld");
			await CreateStoredProcedure(client, "spSetNorthAmerica");
			await CreateStoredProcedure(client, "spEnsureUniqueId");
			await CreateStoredProcedure(client, "spBulkInsert");
			await CreateStoredProcedure(client, "spSelectCount");
			await CreateStoredProcedure(client, "spBulkDelete");
		}

		private async static Task<StoredProcedure> CreateStoredProcedure(DocumentClient client, string sprocId)
		{
			var sprocBody = File.ReadAllText(@"..\..\Server\" + sprocId + ".js");

			var sprocDefinition = new StoredProcedure
			{
				Id = sprocId,
				Body = sprocBody
			};

			var result = await client.CreateStoredProcedureAsync(_collection.SelfLink, sprocDefinition);
			var sproc = result.Resource;
			Console.WriteLine("Created stored procedure {0}; RID: {1}", sproc.Id, sproc.ResourceId);

			return result;
		}

		// View stored procedures

		private static void ViewStoredProcedures(DocumentClient client)
		{
			Console.WriteLine();
			Console.WriteLine(">>> View Stored Procedures <<<");
			Console.WriteLine();

			var sprocs = client
				.CreateStoredProcedureQuery(_collection.StoredProceduresLink)
				.ToList();

			foreach (var sproc in sprocs)
			{
				Console.WriteLine("Stored procedure {0}; RID: {1}", sproc.Id, sproc.ResourceId);
			}
		}

		// Execute stored procedures

		private async static Task ExecuteStoredProcedures(DocumentClient client)
		{
			await Execute_spHelloWorld(client);
			await Execute_spSetNorthAmerica1(client);
			await Execute_spSetNorthAmerica2(client);
			await Execute_spSetNorthAmerica3(client);
			await Execute_spEnsureUniqueId(client);
			await Execute_spBulkInsert(client);
			await Execute_spSelectCount(client);
			await Execute_spBulkDelete(client);
		}

		private async static Task Execute_spHelloWorld(DocumentClient client)
		{
			Console.WriteLine();
			Console.WriteLine("Execute spHelloWorld stored procedure");

			var result1 = await ExecuteStoredProcedure<string>(client, "spHelloWorld");

			Console.WriteLine("Result: {0}", result1);
		}

		private async static Task Execute_spSetNorthAmerica1(DocumentClient client)
		{
			Console.WriteLine();
			Console.WriteLine("Execute spSetNorthAmerica (country = United States)");

			// Should succeed with isNorthAmerica = true
			dynamic documentDefinition = new
			{
				name = "John Doe",
				address = new
				{
					countryRegionName = "United States"
				}
			};
			dynamic document = await ExecuteStoredProcedure<object>(client, "spSetNorthAmerica", documentDefinition, true);

			Console.WriteLine("Result:\r\n Id = {0}\r\n Country = {1}\r\n Is North America = {2}",
				document.id,
				document.address.countryRegionName,
				document.address.isNorthAmerica);

			string documentLink = document._self;
			await client.DeleteDocumentAsync(documentLink);
		}

		private async static Task Execute_spSetNorthAmerica2(DocumentClient client)
		{
			Console.WriteLine();
			Console.WriteLine("Execute spSetNorthAmerica (country = Germany)");

			// Should succeed with isNorthAmerica = false
			dynamic documentDefinition = new
			{
				name = "John Doe",
				address = new
				{
					countryRegionName = "Germany"
				}
			};
			dynamic document = await ExecuteStoredProcedure<object>(client, "spSetNorthAmerica", documentDefinition, true);

			// Deserialize new document as Dictionary (use key/value pairs to access dynamic properties)
			var resultDict = (Dictionary<string, object>)(new JavaScriptSerializer()).Deserialize(document.ToString(), typeof(Dictionary<string, object>));
			var addressDict = resultDict["address"] as Dictionary<string, object>;

			Console.WriteLine("Result:\r\n Id = {0}\r\n Country = {1}\r\n Is North America = {2}",
				resultDict["id"],
				addressDict["countryRegionName"],
				addressDict["isNorthAmerica"]);

			string documentLink = document._self;
			await client.DeleteDocumentAsync(documentLink);
		}

		private async static Task Execute_spSetNorthAmerica3(DocumentClient client)
		{
			Console.WriteLine();
			Console.WriteLine("Execute spSetNorthAmerica (no country)");

			// Should fail with no country and enforceSchema = true
			try
			{
				dynamic documentDefinition = new
				{
					name = "James Smith"
				};
				dynamic document = await ExecuteStoredProcedure<object>(client, "spSetNorthAmerica", documentDefinition, true);
			}
			catch (DocumentClientException ex)
			{
				var message = ParseDocumentClientExceptionMessage(ex);
				Console.WriteLine("Error: {0}", message);
			}
		}

		private static string ParseDocumentClientExceptionMessage(DocumentClientException ex)
		{
			var errorMessage = string.Empty;
			try
			{
				var message = ex.Message;
				var openBrace = message.IndexOf('{');
				var closeBrace = message.IndexOf('}', openBrace);
				var errorsJson = message.Substring(openBrace, closeBrace - openBrace + 1);
				dynamic errorsObj = JObject.Parse(errorsJson);
				foreach (var error in errorsObj.Errors)
				{
					errorMessage += error;
				}
			}
			catch
			{
				errorMessage = ex.Message;
			}

			return errorMessage;
		}

		private async static Task Execute_spEnsureUniqueId(DocumentClient client)
		{
			Console.WriteLine();
			Console.WriteLine("Execute spEnsureUniqueId");

			dynamic documentDefinition1 = new { id = "DUPEJ", name = "James Dupe" };
			dynamic documentDefinition2 = new { id = "DUPEJ", name = "John Dupe" };
			dynamic documentDefinition3 = new { id = "DUPEJ", name = "Justin Dupe" };

			var document1 = await ExecuteStoredProcedure<object>(client, "spEnsureUniqueId", documentDefinition1);
			Console.WriteLine("New document ID: {0}", document1.id);

			var document2 = await ExecuteStoredProcedure<object>(client, "spEnsureUniqueId", documentDefinition2);
			Console.WriteLine("New document ID: {0}", document2.id);

			var document3 = await ExecuteStoredProcedure<object>(client, "spEnsureUniqueId", documentDefinition3);
			Console.WriteLine("New document ID: {0}", document3.id);

			// cleanup
			var sql = "SELECT VALUE c._self FROM c WHERE STARTSWITH(c.id, 'DUPEJ') = true";
			var documentLinks = client.CreateDocumentQuery(_collection.SelfLink, sql).AsEnumerable();

			foreach (string documentLink in documentLinks)
			{
				await client.DeleteDocumentAsync(documentLink);
			}
		}

		private async static Task Execute_spBulkInsert(DocumentClient client)
		{
			Console.WriteLine();
			Console.WriteLine("Execute spBulkInsert");

			var docs = new List<dynamic>();
			var totalNewDocCount = 1000;
			for (var i = 1; i <= totalNewDocCount; i++)
			{
				dynamic newDoc = new
				{
					name = string.Format("Bulk inserted doc {0}", i)
				};
				docs.Add(newDoc);
			}

			var totalInsertedCount = 0;
			while (totalInsertedCount < totalNewDocCount)
			{
				var insertedCount = await ExecuteStoredProcedure<int>(client, "spBulkInsert", docs);
				totalInsertedCount += insertedCount;
				Console.WriteLine("Inserted {0} documents ({1} total, {2} remaining)", insertedCount, totalInsertedCount, totalNewDocCount - totalInsertedCount);
				docs = docs.GetRange(insertedCount, docs.Count - insertedCount);
			}
		}

		private async static Task Execute_spSelectCount(DocumentClient client)
		{
			Console.WriteLine();
			Console.WriteLine("Execute spSelectCount stored procedure");

			// count all documents that satisfy filter
			var filter = "SELECT VALUE c._self FROM c WHERE STARTSWITH(c.name, 'Bulk inserted doc ') = true";
			var count = await Execute_spSelectCount(client, filter);
			Console.WriteLine("Total bulk inserted count: {0}", count);
			Console.WriteLine();

			// count all documents in the entire collection
			count = await Execute_spSelectCount(client, null);
			Console.WriteLine("Total document count: {0}", count);
			Console.WriteLine();
		}

		private static async Task<int> Execute_spSelectCount(DocumentClient client, string sql)
		{
			string continuationToken = null;
			var totalCount = 0;
			var loop = true;
			while (loop)
			{
				var result = await ExecuteStoredProcedure<spSelectCountResponse>(client, "spSelectCount", sql, continuationToken);
				continuationToken = result.ContinuationToken;
				var count = result.Count;
				totalCount += count;
				Console.WriteLine("Counted {0} documents ({1} total, cont: {2})", count, totalCount, continuationToken);
				loop = (continuationToken != null);
			}

			return totalCount;
		}

		private async static Task Execute_spBulkDelete(DocumentClient client)
		{
			Console.WriteLine();
			Console.WriteLine("Execute spBulkDelete");

			// delete all documents that satisfy filter
			var sql = "SELECT VALUE c._self FROM c WHERE STARTSWITH(c.name, 'Bulk inserted doc ') = true";
			var count = await Execute_spBulkDelete(client, sql);
			Console.WriteLine("Deleted bulk inserted documents; count: {0}", count);
			Console.WriteLine();

			// delete all documents in the entire collection
			//count = await Execute_spBulkDelete(client, "SELECT VALUE c._self FROM c");
			//Console.WriteLine("Deleted all documents in collection; count: {0}", count);
			//Console.WriteLine();
		}

		private async static Task<int> Execute_spBulkDelete(DocumentClient client, string sql)
		{
			var continuationFlag = true;
			var totalDeletedCount = 0;
			while (continuationFlag)
			{
				var result = await ExecuteStoredProcedure<spBulkDeleteResponse>(client, "spBulkDelete", sql);
				continuationFlag = result.ContinuationFlag;
				var deletedCount = result.Count;
				totalDeletedCount += deletedCount;
				Console.WriteLine("Deleted {0} documents ({1} total, more: {2})", deletedCount, totalDeletedCount, continuationFlag);
			}

			return totalDeletedCount;
		}

		private async static Task<T> ExecuteStoredProcedure<T>(DocumentClient client, string sprocId, params dynamic[] sprocParams)
		{
			var query = new SqlQuerySpec
			{
				QueryText = "SELECT * FROM c WHERE c.id = @id",
				Parameters = new SqlParameterCollection { new SqlParameter { Name = "@id", Value = sprocId } }
			};

			StoredProcedure sproc = client
				.CreateStoredProcedureQuery(_collection.StoredProceduresLink, query)
				.AsEnumerable()
				.First();

			while (true)
			{
				try
				{
					var result = await client.ExecuteStoredProcedureAsync<T>(sproc.SelfLink, sprocParams);

					Console.WriteLine("Executed stored procedure: {0}", sprocId);
					return result;
				}
				catch (DocumentClientException ex)
				{
					if ((int)ex.StatusCode == 429)
					{
						Console.WriteLine("  ...retry in {0}", ex.RetryAfter);
						Thread.Sleep(ex.RetryAfter);
						continue;
					}
					throw ex;
				}
			}
		}

		// Delete stored procedures

		private async static Task DeleteStoredProcedures(DocumentClient client)
		{
			Console.WriteLine();
			Console.WriteLine(">>> Delete Stored Procedures <<<");
			Console.WriteLine();

			await DeleteStoredProcedure(client, "spHelloWorld");
			await DeleteStoredProcedure(client, "spSetNorthAmerica");
			await DeleteStoredProcedure(client, "spEnsureUniqueId");
			await DeleteStoredProcedure(client, "spBulkInsert");
			await DeleteStoredProcedure(client, "spSelectCount");
			await DeleteStoredProcedure(client, "spBulkDelete");
		}

		private async static Task DeleteStoredProcedure(DocumentClient client, string sprocId)
		{
			var sproc = client
				.CreateStoredProcedureQuery(_collection.StoredProceduresLink)
				.AsEnumerable()
				.First(s => s.Id == sprocId);

			await client.DeleteStoredProcedureAsync(sproc.SelfLink);

			Console.WriteLine("Deleted stored procedure: {0}", sprocId);
		}

	}

	public class spSelectCountResponse
	{
		[JsonProperty(PropertyName = "count")]
		public int Count { get; set; }
		[JsonProperty(PropertyName = "continuationToken")]
		public string ContinuationToken { get; set; }
	}

	public class spBulkDeleteResponse
	{
		[JsonProperty(PropertyName = "count")]
		public int Count { get; set; }
		[JsonProperty(PropertyName = "continuationFlag")]
		public bool ContinuationFlag { get; set; }
	}

}
