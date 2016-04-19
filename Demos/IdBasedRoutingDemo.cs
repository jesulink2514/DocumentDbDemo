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
	public static class IdBasedRoutingDemo
	{
		public async static Task Run()
		{
			Debugger.Break();

			var endpoint = ConfigurationManager.AppSettings["DocDbEndpoint"];
			var masterKey = ConfigurationManager.AppSettings["DocDbMasterKey"];

			// *** Before ID-Based Routing (8/13/2015) ***

			// Create a database
			using (var client = new DocumentClient(new Uri(endpoint), masterKey))
			{
				await client.CreateDatabaseAsync(new Database { Id = "MyNewDb" });
			}

			// Create a collection without knowing the database self-link
			using (var client = new DocumentClient(new Uri(endpoint), masterKey))
			{
				Database database = client
					.CreateDatabaseQuery("SELECT * FROM c WHERE c.id = 'MyNewDb'")
					.AsEnumerable()
					.First();

				await client.CreateDocumentCollectionAsync(database.SelfLink, new DocumentCollection { Id = "MyNewColl" });
			}

			// Create a document without knowing the database or collection self-link
			using (var client = new DocumentClient(new Uri(endpoint), masterKey))
			{
				Database database = client
					.CreateDatabaseQuery("SELECT * FROM c WHERE c.id = 'MyNewDb'")
					.AsEnumerable()
					.First();

				DocumentCollection collection = client
					.CreateDocumentCollectionQuery(database.CollectionsLink, "SELECT * FROM c WHERE c.id = 'MyNewColl'")
					.AsEnumerable()
					.First();

				await client.CreateDocumentAsync(collection.SelfLink, new Document { Id = "MyNewDoc" });
			}

			// Query for a document by its ID without knowing the database or collection self-link
			using (var client = new DocumentClient(new Uri(endpoint), masterKey))
			{
				Database database = client
					.CreateDatabaseQuery("SELECT * FROM c WHERE c.id = 'MyNewDb'")
					.AsEnumerable()
					.First();

				DocumentCollection collection = client
					.CreateDocumentCollectionQuery(database.CollectionsLink, "SELECT * FROM c WHERE c.id = 'MyNewColl'")
					.AsEnumerable()
					.First();

				Document document = client
					.CreateDocumentQuery(collection.DocumentsLink, "SELECT * FROM c WHERE c.id = 'MyNewDoc'")
					.AsEnumerable()
					.First();
			}

			// Delete a document, collection, and database, without knowing any of their self-links
			using (var client = new DocumentClient(new Uri(endpoint), masterKey))
			{
				Database database = client
					.CreateDatabaseQuery("SELECT * FROM c WHERE c.id = 'MyNewDb'")
					.AsEnumerable()
					.First();

				DocumentCollection collection = client
					.CreateDocumentCollectionQuery(database.CollectionsLink, "SELECT * FROM c WHERE c.id = 'MyNewColl'")
					.AsEnumerable()
					.First();

				Document document = client
					.CreateDocumentQuery(collection.DocumentsLink, "SELECT * FROM c WHERE c.id = 'MyNewDoc'")
					.AsEnumerable()
					.First();

				await client.DeleteDocumentAsync(document.SelfLink);
				await client.DeleteDocumentCollectionAsync(collection.SelfLink);
				await client.DeleteDatabaseAsync(database.SelfLink);
			}

			// *** After ID-Based Routing (8/13/2015) ***

			// Create a database
			using (var client = new DocumentClient(new Uri(endpoint), masterKey))
			{
				await client.CreateDatabaseAsync(new Database { Id = "MyNewDb" });
			}

			// Create a collection without knowing the database self-link
			using (var client = new DocumentClient(new Uri(endpoint), masterKey))
			{
				await client.CreateDocumentCollectionAsync("dbs/MyNewDb", new DocumentCollection { Id = "MyNewColl" });
			}

			// Create a document without knowing the database or collection self-link
			using (var client = new DocumentClient(new Uri(endpoint), masterKey))
			{
				await client.CreateDocumentAsync("dbs/MyNewDb/colls/MyNewColl", new Document { Id = "MyNewDoc" });
			}

			// Query for a document by its ID without knowing the database or collection self-link
			using (var client = new DocumentClient(new Uri(endpoint), masterKey))
			{
				Document document = client
					.CreateDocumentQuery("dbs/MyNewDb/colls/MyNewColl/docs", "SELECT * FROM c WHERE c.id = 'MyNewDoc'")
					.AsEnumerable()
					.First();
			}

			// Delete a document, collection, and database, without knowing any of their self-links
			using (var client = new DocumentClient(new Uri(endpoint), masterKey))
			{
				await client.DeleteDocumentAsync("dbs/MyNewDb/colls/MyNewColl/docs/MyNewDoc");
				await client.DeleteDocumentCollectionAsync("dbs/MyNewDb/colls/MyNewColl");
				await client.DeleteDatabaseAsync("dbs/MyNewDb");
			}

			// Use UriFactory to automatically construct a URL-encoded ID-based self-link
			using (var client = new DocumentClient(new Uri(endpoint), masterKey))
			{
				await client.CreateDatabaseAsync(new Database { Id = "MyNewDb" });
				await client.CreateDocumentCollectionAsync(UriFactory.CreateDatabaseUri("MyNewDb"), new DocumentCollection { Id = "MyNewColl" });
				await client.CreateDocumentAsync(UriFactory.CreateCollectionUri("MyNewDb", "MyNewColl"), new Document { Id = "MyNewDoc" });

				Document document = client
					.CreateDocumentQuery(UriFactory.CreateCollectionUri("MyNewDb", "MyNewColl"), "SELECT * FROM c WHERE c.id = 'MyNewDoc'")
					.AsEnumerable()
					.First();
				
				await client.DeleteDocumentAsync(UriFactory.CreateDocumentUri("MyNewDb", "MyNewColl", "MyNewDoc"));
				await client.DeleteDocumentCollectionAsync(UriFactory.CreateCollectionUri("MyNewDb", "MyNewColl"));
				await client.DeleteDatabaseAsync(UriFactory.CreateDatabaseUri("MyNewDb"));
			}

		}

	}
}
