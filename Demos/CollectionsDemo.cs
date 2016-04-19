using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using System;
using System.Configuration;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace DocDb.DotNetSdk.Demos
{
	public static class CollectionsDemo
	{
		private static Database _database;

		public async static Task Run()
		{
			Debugger.Break();

			var endpoint = ConfigurationManager.AppSettings["DocDbEndpoint"];
			var masterKey = ConfigurationManager.AppSettings["DocDbMasterKey"];

			using (var client = new DocumentClient(new Uri(endpoint), masterKey))
			{
				_database = client.CreateDatabaseQuery("SELECT * FROM c WHERE c.id = 'mydb'").AsEnumerable().First();

				ViewCollections(client);

				await CreateCollection(client, "MyCollection1");
				await CreateCollection(client, "MyCollection2", "S2");
				ViewCollections(client);

				await DeleteCollection(client, "MyCollection1");
				await DeleteCollection(client, "MyCollection2");
			}
		}

		private static void ViewCollections(DocumentClient client)
		{
			Console.WriteLine();
			Console.WriteLine(">>> View Collections in {0} <<<", _database.Id);

			var collections = client
				.CreateDocumentCollectionQuery(_database.CollectionsLink)
				.ToList();

			var i = 0;
			foreach (var collection in collections)
			{
				i++;
				Console.WriteLine();
				Console.WriteLine("Collection #{0}", i);
				ViewCollection(collection);
			}

			Console.WriteLine();
			Console.WriteLine("Total collections in database {0}: {1}", _database.Id, collections.Count);
		}

		private static void ViewCollection(DocumentCollection collection)
		{
			Console.WriteLine("    Collection ID: {0} ", collection.Id);
			Console.WriteLine("      Resource ID: {0} ", collection.ResourceId);
			Console.WriteLine("        Self Link: {0} ", collection.SelfLink);
			Console.WriteLine("   Documents Link: {0} ", collection.DocumentsLink);
			Console.WriteLine("        UDFs Link: {0} ", collection.UserDefinedFunctionsLink);
			Console.WriteLine(" StoredProcs Link: {0} ", collection.StoredProceduresLink);
			Console.WriteLine("    Triggers Link: {0} ", collection.TriggersLink);
			Console.WriteLine("        Timestamp: {0} ", collection.Timestamp);
		}

		private async static Task CreateCollection(DocumentClient client, string collectionId, string offerType = "S1")
		{
			Console.WriteLine();
			Console.WriteLine(">>> Create Collection {0} in {1} <<<", collectionId, _database.Id);

			var collectionDefinition = new DocumentCollection { Id = collectionId };
			var options = new RequestOptions { OfferType = offerType };
			var result = await client.CreateDocumentCollectionAsync(_database.SelfLink, collectionDefinition, options);
			var collection = result.Resource;

			Console.WriteLine("Created new collection");
			ViewCollection(collection);
		}

		private async static Task DeleteCollection(DocumentClient client, string collectionId)
		{
			Console.WriteLine();
			Console.WriteLine(">>> Delete Collection {0} in {1} <<<", collectionId, _database.Id);

			var query = new SqlQuerySpec
			{
				QueryText = "SELECT * FROM c WHERE c.id = @id",
				Parameters = new SqlParameterCollection { new SqlParameter { Name = "@id", Value = collectionId } }
			};

			DocumentCollection collection = client.CreateDocumentCollectionQuery(_database.SelfLink, query).AsEnumerable().First();

			await client.DeleteDocumentCollectionAsync(collection.SelfLink);

			Console.WriteLine("Deleted collection {0} from database {1}", collectionId, _database.Id);
		}

	}
}
