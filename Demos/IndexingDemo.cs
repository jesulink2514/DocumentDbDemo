using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using System;
using System.Collections.ObjectModel;
using System.Configuration;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace DocDb.DotNetSdk.Demos
{
	public static class IndexingDemo
	{
		public async static Task Run()
		{
			Debugger.Break();

			var endpoint = ConfigurationManager.AppSettings["DocDbEndpoint"];
			var masterKey = ConfigurationManager.AppSettings["DocDbMasterKey"];

			using (var client = new DocumentClient(new Uri(endpoint), masterKey))
			{
				await AutomaticIndexing(client);
				await ManualIndexing(client);
				await SetIndexPaths(client);
			}
		}

		private async static Task AutomaticIndexing(DocumentClient client)
		{
			Console.WriteLine();
			Console.WriteLine(">>> Override Automatic Indexing <<<");

			// Create collection with automatic indexing
			var collectionDefinition = new DocumentCollection
			{
				Id = "autoindexing"
			};
			var collection = await client.CreateDocumentCollectionAsync("dbs/mydb", collectionDefinition);

			// Add a document (indexed)
			dynamic indexedDocumentDefinition = new
			{
				id = "JOHN",
				firstName = "John",
				lastName = "Doe",
				addressLine = "123 Main Street",
				city = "Brooklyn",
				state = "New York",
				zip = "11229",
			};
			Document indexedDocument = await client
				.CreateDocumentAsync("dbs/mydb/colls/autoindexing", indexedDocumentDefinition);

			// Add another document (request no indexing)
			dynamic unindexedDocumentDefinition = new
			{
				id = "JANE",
				firstName = "Jane",
				lastName = "Doe",
				addressLine = "123 Main Street",
				city = "Brooklyn",
				state = "New York",
				zip = "11229",
			};
			Document unindexedDocument = await client
				.CreateDocumentAsync(
					"dbs/mydb/colls/autoindexing",
					unindexedDocumentDefinition,
					new RequestOptions { IndexingDirective = IndexingDirective.Exclude });

			// Unindexed document won't get returned when querying on non-ID (or self-link) property
			var doeDocs = client.CreateDocumentQuery("dbs/mydb/colls/autoindexing", "SELECT * FROM c WHERE c.lastName = 'Doe'").ToList();
			Console.WriteLine("Documents WHERE lastName = 'Doe': {0}", doeDocs.Count);

			// Unindexed document will get returned when using no WHERE clause
			var allDocs = client.CreateDocumentQuery("dbs/mydb/colls/autoindexing", "SELECT * FROM c").ToList();
			Console.WriteLine("All documents: {0}", allDocs.Count);

			// Unindexed document will get returned when querying by ID (or self-link) property
			Document janeDoc = client
				.CreateDocumentQuery("dbs/mydb/colls/autoindexing", "SELECT * FROM c WHERE c.id = 'JANE'")
				.AsEnumerable()
				.FirstOrDefault();

			Console.WriteLine("Unindexed document self-link: {0}", janeDoc.SelfLink);

			// Delete the collection
			await client.DeleteDocumentCollectionAsync("dbs/mydb/colls/autoindexing");
		}

		private async static Task ManualIndexing(DocumentClient client)
		{
			Console.WriteLine();
			Console.WriteLine(">>> Manual Indexing <<<");

			// Create collection with manual indexing
			var collectionDefinition = new DocumentCollection
			{
				Id = "manualindexing",
				IndexingPolicy = new IndexingPolicy
				{
					Automatic = false,
				},
			};
			var collection = await client.CreateDocumentCollectionAsync("dbs/mydb", collectionDefinition);

			// Add a document (unindexed)
			dynamic unindexedDocumentDefinition = new
			{
				id = "JOHN",
				firstName = "John",
				lastName = "Doe",
				addressLine = "123 Main Street",
				city = "Brooklyn",
				state = "New York",
				zip = "11229",
			};
			Document unindexedDocument = await client
				.CreateDocumentAsync("dbs/mydb/colls/manualindexing", unindexedDocumentDefinition);

			// Add another document (request indexing)
			dynamic indexedDocumentDefinition = new
			{
				id = "JANE",
				firstName = "Jane",
				lastName = "Doe",
				addressLine = "123 Main Street",
				city = "Brooklyn",
				state = "New York",
				zip = "11229",
			};
			Document indexedDocument = await client
				.CreateDocumentAsync(
					"dbs/mydb/colls/manualindexing",
					indexedDocumentDefinition,
					new RequestOptions { IndexingDirective = IndexingDirective.Include });

			// Unindexed document won't get returned when querying on non-ID (or self-link) property
			var doeDocs = client.CreateDocumentQuery("dbs/mydb/colls/manualindexing", "SELECT * FROM c WHERE c.lastName = 'Doe'").ToList();
			Console.WriteLine("Documents WHERE lastName = 'Doe': {0}", doeDocs.Count);

			// Unindexed document will get returned when using no WHERE clause
			var allDocs = client.CreateDocumentQuery("dbs/mydb/colls/manualindexing", "SELECT * FROM c").ToList();
			Console.WriteLine("All documents: {0}", allDocs.Count);

			// Unindexed document will get returned when querying by ID (or self-link) property
			Document johnDoc = client
				.CreateDocumentQuery("dbs/mydb/colls/manualindexing", "SELECT * FROM c WHERE c.id = 'JOHN'")
				.AsEnumerable()
				.FirstOrDefault();

			Console.WriteLine("Unindexed document self-link: {0}", johnDoc.SelfLink);

			await client.DeleteDocumentCollectionAsync("dbs/mydb/colls/manualindexing");
		}

		private async static Task SetIndexPaths(DocumentClient client)
		{
			Console.WriteLine();
			Console.WriteLine(">>> Set Custom Index Paths <<<");

			// Create collection with custom indexing paths
			var collectionDefinition = new DocumentCollection
			{
				Id = "customindexing",
				IndexingPolicy = new IndexingPolicy
				{
					IncludedPaths = new Collection<IncludedPath> 
					{
						// The Title property in the root is the only string property we need to sort on
						new IncludedPath
						{
							Path = "/title/?",
							Indexes = new Collection<Index>
							{
								new RangeIndex(DataType.String)
							}
						},
						// Every property (also the Title) gets a hash index on strings, and a range index on numbers
						new IncludedPath
						{
							Path = "/*",
							Indexes = new Collection<Index>
							{
								new HashIndex(DataType.String),
								new RangeIndex(DataType.Number),
							}
						}
					}
				},
			};
			var collection = await client.CreateDocumentCollectionAsync("dbs/mydb", collectionDefinition);

			// Add some documents
			dynamic doc1Definition = new
			{
				id = "SW4",
				title = "Star Wars IV - A New Hope",
				rank = 600,
				category = "Sci-Fi",
			};
			Document doc1 = await client.CreateDocumentAsync("dbs/mydb/colls/customindexing", doc1Definition);

			dynamic doc2Definition = new
			{
				id = "GF",
				title = "Godfather",
				rank = 500,
				category = "Crime Drama"
			};
			Document doc2 = await client.CreateDocumentAsync("dbs/mydb/colls/customindexing", doc2Definition);

			dynamic doc3Definition = new
			{
				id = "LOTR1",
				title = "Lord Of The Rings - Fellowship of the Ring",
				rank = 700,
				category = "Fantasy"
			};
			Document doc3 = await client.CreateDocumentAsync("dbs/mydb/colls/customindexing", doc3Definition);

			// Works with range index on title property strings
			var byTitle = client.CreateDocumentQuery("dbs/mydb/colls/customindexing", "SELECT * FROM c ORDER BY c.title").ToList();

			// Doesn't works without range index on category property strings (returns 0 documents, but doesn't throw error!)
			var byCategory = client.CreateDocumentQuery("dbs/mydb/colls/customindexing", "SELECT * FROM c ORDER BY c.category").ToList();

			// Works with range index on rank property numbers
			var tryRankSort = client.CreateDocumentQuery("dbs/mydb/colls/customindexing", "SELECT * FROM c ORDER BY c.rank").ToList();

			// Delete the collection
			await client.DeleteDocumentCollectionAsync("dbs/mydb/colls/customindexing");
		}

	}
}
