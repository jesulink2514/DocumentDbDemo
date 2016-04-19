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
	public static class TriggersDemo
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

				await CreateTriggers(client);

				ViewTriggers(client);

				await Execute_trgEnsureUniqueId(client);
				await Execute_trgUpdateMetadata(client);

				await DeleteTriggers(client);
			}
		}

		private async static Task CreateTriggers(DocumentClient client)
		{
			Console.WriteLine();
			Console.WriteLine(">>> Create Triggers <<<");
			Console.WriteLine();

			// Create pre-trigger
			var trgEnsureUniqueId = File.ReadAllText(@"..\..\Server\trgEnsureUniqueId.js");
			await CreateTrigger(client, "trgEnsureUniqueId", trgEnsureUniqueId, TriggerType.Pre, TriggerOperation.Create);

			// Create post-trigger
			var trgUpdateMetadata = File.ReadAllText(@"..\..\Server\trgUpdateMetadata.js");
			await CreateTrigger(client, "trgUpdateMetadata", trgUpdateMetadata, TriggerType.Post, TriggerOperation.All);
		}

		private async static Task<Trigger> CreateTrigger(DocumentClient client, string triggerId, string triggerBody, TriggerType triggerType, TriggerOperation triggerOperation)
		{
			var triggerDefinition = new Trigger
			{
				Id = triggerId,
				Body = triggerBody,
				TriggerType = triggerType,
				TriggerOperation = triggerOperation
			};

			var result = await client.CreateTriggerAsync(_collection.SelfLink, triggerDefinition);
			var trigger = result.Resource;
			Console.WriteLine("Created trigger {0}; RID: {1}", trigger.Id, trigger.ResourceId);

			return trigger;
		}

		private static void ViewTriggers(DocumentClient client)
		{
			Console.WriteLine();
			Console.WriteLine(">>> View Triggers <<<");
			Console.WriteLine();

			var triggers = client
				.CreateTriggerQuery(_collection.TriggersLink)
				.ToList();

			foreach (var trigger in triggers)
			{
				Console.WriteLine("Trigger: {0}; RID: {1}; Type: {2}; Operation: {3}", trigger.Id, trigger.ResourceId, trigger.TriggerType, trigger.TriggerOperation);
			}
		}

		private async static Task Execute_trgEnsureUniqueId(DocumentClient client)
		{
			dynamic newDoc1 = new { id = "DUPEJ", name = "James Dupe" };
			dynamic newDoc2 = new { id = "DUPEJ", name = "John Dupe" };
			dynamic newDoc3 = new { id = "DUPEJ", name = "Justin Dupe" };

			var result1 = await client.CreateDocumentAsync(_collection.SelfLink, newDoc1, new RequestOptions { PreTriggerInclude = new[] { "trgEnsureUniqueId" } });
			Console.WriteLine("New document ID: {0}", ((Document)result1).Id);

			var result2 = await client.CreateDocumentAsync(_collection.SelfLink, newDoc2, new RequestOptions { PreTriggerInclude = new[] { "trgEnsureUniqueId" } });
			Console.WriteLine("New document ID: {0}", ((Document)result2).Id);

			var result3 = await client.CreateDocumentAsync(_collection.SelfLink, newDoc3, new RequestOptions { PreTriggerInclude = new[] { "trgEnsureUniqueId" } });
			Console.WriteLine("New document ID: {0}", ((Document)result3).Id);

			// cleanup
			var sql = "SELECT VALUE c._self FROM c WHERE STARTSWITH(c.id, 'DUPEJ') = true";
			var documentLinks = client.CreateDocumentQuery(_collection.SelfLink, sql).AsEnumerable();

			foreach (string documentLink in documentLinks)
			{
				await client.DeleteDocumentAsync(documentLink);
			}
		}

		private async static Task Execute_trgUpdateMetadata(DocumentClient client)
		{
			// Creating a new document also updates metadata document (or creates it, if it doesn't exist yet)
			dynamic newDoc = new { id = "NEWDOC", name = "Test" };
			var result = await client.CreateDocumentAsync(_collection.SelfLink, newDoc, new RequestOptions { PostTriggerInclude = new[] { "trgUpdateMetadata" } });

			// Retrieve the metadata document (has ID of last inserted data document)
			var metadoc = client.CreateDocumentQuery(_collection.SelfLink, "SELECT * FROM c WHERE c.id = '_metadata'").AsEnumerable().First();
			Console.WriteLine("Updated metadata: {0}", metadoc);

			// cleanup
			var sql = "SELECT VALUE c._self FROM c WHERE c.id IN('NEWDOC', '_metadata')";
			var documentLinks = client.CreateDocumentQuery(_collection.SelfLink, sql).ToList();
			
			foreach (string documentLink in documentLinks)
			{
				await client.DeleteDocumentAsync(documentLink);
			}
		}

		private async static Task DeleteTriggers(DocumentClient client)
		{
			Console.WriteLine();
			Console.WriteLine(">>> Delete Triggers <<<");
			Console.WriteLine();

			await DeleteTrigger(client, "trgEnsureUniqueId");
			await DeleteTrigger(client, "trgUpdateMetadata");
		}

		private async static Task DeleteTrigger(DocumentClient client, string triggerId)
		{
			var trigger = client
				.CreateTriggerQuery(_collection.TriggersLink)
				.AsEnumerable()
				.First(t => t.Id == triggerId);

			await client.DeleteTriggerAsync(trigger.SelfLink);

			Console.WriteLine("Deleted trigger: {0}", triggerId);
		}

	}
}
