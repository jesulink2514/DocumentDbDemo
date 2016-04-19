using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using System.Web.Script.Serialization;

namespace DocDb.DotNetSdk.Demos
{
	public static class DocumentsDemo
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

				await CreateDocuments(client);

				QueryDocumentsWithSql(client);
				await QueryDocumentsWithPaging(client);
				QueryDocumentsWithLinq(client);

				await ReplaceDocuments(client);

				await DeleteDocuments(client);
			}
		}

		private async static Task CreateDocuments(DocumentClient client)
		{
			Console.WriteLine();
			Console.WriteLine(">>> Create Documents <<<");
			Console.WriteLine();

			dynamic document1Definition = new
			{
				name = "New Customer 1",
				address = new
				{
					addressType = "Main Office",
					addressLine1 = "123 Main Street",
					location = new
					{
						city = "Brooklyn",
						stateProvinceName = "New York"
					},
					postalCode = "11229",
					countryRegionName = "United States"
				},
			};

			Document document1 = await CreateDocument(client, document1Definition);
			Console.WriteLine("Created document {0} from dynamic object", document1.Id);
			Console.WriteLine();

			var document2Definition = @"
			{
				""name"": ""New Customer 2"",
				""address"": {
					""addressType"": ""Main Office"",
					""addressLine1"": ""123 Main Street"",
					""location"": {
						""city"": ""Brooklyn"",
						""stateProvinceName"": ""New York""
					},
					""postalCode"": ""11229"",
					""countryRegionName"": ""United States""
				}
			}";

			Document document2 = await CreateDocument(client, document2Definition);
			Console.WriteLine("Created document {0} from JSON string", document2.Id);
			Console.WriteLine();

			var document3Definition = new Customer
			{
				Name = "New Customer 3",
				Address = new Address
				{
					AddressType = "Main Office",
					AddressLine1 = "123 Main Street",
					Location = new Location
					{
						City = "Brooklyn",
						StateProvinceName = "New York"
					},
					PostalCode = "11229",
					CountryRegionName = "United States"
				},
			};

			Document document3 = await CreateDocument(client, document3Definition);
			Console.WriteLine("Created document {0} from typed object", document3.Id);
			Console.WriteLine();
		}

		private async static Task<Document> CreateDocument(DocumentClient client, string documentJson)
		{
			var documentObject = new JavaScriptSerializer().Deserialize<object>(documentJson);
			return await CreateDocument(client, documentObject);
		}

		private async static Task<Document> CreateDocument(DocumentClient client, object documentObject)
		{
			var result = await client.CreateDocumentAsync(_collection.SelfLink, documentObject);
			var document = result.Resource;
			Console.WriteLine("Created new document: {0}\r\n{1}", document.Id, document);
			return result;
		}

		private static void QueryDocumentsWithSql(DocumentClient client)
		{
			Console.WriteLine();
			Console.WriteLine(">>> Query Documents (SQL) <<<");
			Console.WriteLine();

			Console.WriteLine("Quering for new customer documents (SQL)");
			var sql = "SELECT * FROM c WHERE STARTSWITH(c.name, 'New Customer') = true";

			// Query for dynamic objects
			var documents = client.CreateDocumentQuery(_collection.SelfLink, sql).ToList();
			Console.WriteLine("Found {0} new documents", documents.Count);
			foreach (var document in documents)
			{
				Console.WriteLine(" Id: {0}; Name: {1};", document.id, document.name);

				// Dynamic object can be converted into a defined type...
				var customer = (Customer)(new JavaScriptSerializer()).Deserialize(document.ToString(), typeof(Customer));
				Console.WriteLine(" City: {0}", customer.Address.Location.City);
			}
			Console.WriteLine();

			// Or query for defined types; e.g., Customer
			var customers = client.CreateDocumentQuery<Customer>(_collection.SelfLink, sql).ToList();
			Console.WriteLine("Found {0} new customers", customers.Count);
			foreach (var customer in customers)
			{
				Console.WriteLine(" Id: {0}; Name: {1};", customer.Id, customer.Name);
				Console.WriteLine(" City: {0}", customer.Address.Location.City);
			}
			Console.WriteLine();

			Console.WriteLine("Quering for all documents (SQL)");
			sql = "SELECT * FROM c";
			documents = client.CreateDocumentQuery(_collection.SelfLink, sql).ToList();

			Console.WriteLine("Found {0} documents", documents.Count);
			foreach (var document in documents)
			{
				Console.WriteLine(" Id: {0}; Name: {1};", document.id, document.name);
			}
			Console.WriteLine();
		}

		private async static Task QueryDocumentsWithPaging(DocumentClient client)
		{
			Console.WriteLine();
			Console.WriteLine(">>> Query Documents (paged results) <<<");
			Console.WriteLine();

			Console.WriteLine("Quering for all documents");
			var sql = "SELECT * FROM c";

			var query = client
				.CreateDocumentQuery(_collection.SelfLink, sql)
				.AsDocumentQuery();

			while (query.HasMoreResults)
			{
				var documents = await query.ExecuteNextAsync();
				foreach (var document in documents)
				{
					Console.WriteLine(" Id: {0}; Name: {1};", document.id, document.name);
				}
			}
			Console.WriteLine();
		}

		private static void QueryDocumentsWithLinq(DocumentClient client)
		{
			Console.WriteLine();
			Console.WriteLine(">>> Query Documents (LINQ) <<<");
			Console.WriteLine();

			Console.WriteLine("Quering for UK customers (LINQ)");
			var q =
				from d in client.CreateDocumentQuery<Customer>(_collection.DocumentsLink)
				where d.Address.CountryRegionName == "United Kingdom"
				select new
				{
					Id = d.Id,
					Name = d.Name,
					City = d.Address.Location.City
				};

			var documents = q.ToList();

			Console.WriteLine("Found {0} UK customers", documents.Count);
			foreach (var document in documents)
			{
				var d = document as dynamic;
				Console.WriteLine(" Id: {0}; Name: {1}; City: {2}", d.Id, d.Name, d.City);
			}
			Console.WriteLine();
		}

		private async static Task ReplaceDocuments(DocumentClient client)
		{
			Console.WriteLine();
			Console.WriteLine(">>> Replace Documents <<<");
			Console.WriteLine();

			Console.WriteLine("Quering for documents with 'isNew' flag");
			var sql = "SELECT * FROM c WHERE c.isNew = true";
			var documents = client.CreateDocumentQuery(_collection.SelfLink, sql).ToList();
			Console.WriteLine("Documents with 'isNew' flag: {0} ", documents.Count);
			Console.WriteLine();

			Console.WriteLine("Quering for documents to be updated");
			sql = "SELECT * FROM c WHERE STARTSWITH(c.name, 'New Customer') = true";
			documents = client.CreateDocumentQuery(_collection.SelfLink, sql).ToList();
			Console.WriteLine("Found {0} documents to be updated", documents.Count);
			foreach (var document in documents)
			{
				document.isNew = true;
				var result = await client.ReplaceDocumentAsync(document._self, document);
				var updatedDocument = result.Resource;
				Console.WriteLine("Updated document 'isNew' flag: {0}", updatedDocument.isNew);
			}
			Console.WriteLine();

			Console.WriteLine("Quering for documents with 'isNew' flag");
			sql = "SELECT * FROM c WHERE c.isNew = true";
			documents = client.CreateDocumentQuery(_collection.SelfLink, sql).ToList();
			Console.WriteLine("Documents with 'isNew' flag: {0}: ", documents.Count);
			Console.WriteLine();
		}

		private async static Task DeleteDocuments(DocumentClient client)
		{
			Console.WriteLine();
			Console.WriteLine(">>> Delete Documents <<<");
			Console.WriteLine();

			Console.WriteLine("Quering for documents to be deleted");
			var sql = "SELECT VALUE c._self FROM c WHERE STARTSWITH(c.name, 'New Customer') = true";
			var documentLinks = client.CreateDocumentQuery<string>(_collection.SelfLink, sql).ToList();
			Console.WriteLine("Found {0} documents to be deleted", documentLinks.Count);
			foreach (var documentLink in documentLinks)
			{
				await client.DeleteDocumentAsync(documentLink);
			}
			Console.WriteLine("Deleted {0} new customer documents", documentLinks.Count);
			Console.WriteLine();
		}

	}
}
