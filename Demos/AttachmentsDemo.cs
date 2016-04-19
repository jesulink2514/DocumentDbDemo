using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using System;
using System.Configuration;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace DocDb.DotNetSdk.Demos
{
	public static class AttachmentsDemo
	{
		public async static Task Run()
		{
			Debugger.Break();

			var endpoint = ConfigurationManager.AppSettings["DocDbEndpoint"];
			var masterKey = ConfigurationManager.AppSettings["DocDbMasterKey"];

			using (var client = new DocumentClient(new Uri(endpoint), masterKey))
			{
				Document document = await CreateWithAttachments(client);

				await QueryWithAttachments(client);

				await client.DeleteDocumentAsync(document.SelfLink);
			}
		}

		private async static Task<Document> CreateWithAttachments(DocumentClient client)
		{
			dynamic documentDefinition = new
			{
				id = "ALBUM152",
				name = "New Customer 99",
				favoriteAlbum = "The worst nightmare",
				tags = new[]
				{
					"arctics",
					"monkeys"
				}
			};

			Document document = await client.CreateDocumentAsync("dbs/mydb/colls/mystore", documentDefinition);
			Console.WriteLine("Created document");
			Console.WriteLine(document);

			using (var fs = new FileStream(@"C:\Demo\piwi.jpg", FileMode.Open))
			{
				var result = await client.CreateAttachmentAsync(document.AttachmentsLink, fs);
				Console.WriteLine("Created attachment #1");
				Console.WriteLine(result.Resource);
			}

			using (var fs = new FileStream(@"C:\Demo\mug.jpg", FileMode.Open))
			{
				var result = await client.CreateAttachmentAsync(document.AttachmentsLink, fs);
				Console.WriteLine("Created attachment #2");
				Console.WriteLine(result.Resource);
			}

			return document;
		}

		private async static Task QueryWithAttachments(DocumentClient client)
		{
			Document document = client
				.CreateDocumentQuery("dbs/mydb/colls/mystore", "SELECT * FROM c WHERE c.id = 'ALBUM152'")
				.AsEnumerable()
				.First();

			var attachments = client
				.CreateAttachmentQuery(document.SelfLink)
				.ToList();

			foreach (var attachment in attachments)
			{
				var response = await client.ReadMediaAsync(attachment.MediaLink);
				var bytes = new byte[response.ContentLength];
				await response.Media.ReadAsync(bytes, 0, (int)response.ContentLength);

				var filename = string.Format(@"C:\Demo\Images\Attachment{0}.jpg", attachment.ResourceId);
				using (var fs = new FileStream(filename, FileMode.CreateNew))
				{
					fs.Write(bytes, 0, bytes.Length);
				}
			}
		}

	}
}
