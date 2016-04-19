using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace DocDb.DotNetSdk.Demos
{
	public static class UsersAndPermissionsDemo
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

				ViewUsers(client);

				var alice = await CreateUser(client, "Alice");
				var tom = await CreateUser(client, "Tom");
				ViewUsers(client);

				ViewPermissions(client, alice);
				ViewPermissions(client, tom);

				string collectionLink = client.CreateDocumentCollectionQuery(_database.SelfLink, "SELECT VALUE c._self FROM c WHERE c.id = 'mystore'").AsEnumerable().First().Value;
				await CreatePermission(client, alice, "Alice Collection Access", PermissionMode.All, collectionLink);
				await CreatePermission(client, tom, "Tom Collection Access", PermissionMode.Read, collectionLink);

				ViewPermissions(client, alice);
				ViewPermissions(client, tom);

				await TestPermissions(client, alice, collectionLink);
				await TestPermissions(client, tom, collectionLink);

				await DeletePermission(client, alice, "Alice Collection Access");
				await DeletePermission(client, tom, "Tom Collection Access");

				await DeleteUser(client, "Alice");
				await DeleteUser(client, "Tom");
			}
		}

		// Users

		private static void ViewUsers(DocumentClient client)
		{
			Console.WriteLine();
			Console.WriteLine(">>> View Users in {0} <<<", _database.Id);

			var users = client.CreateUserQuery(_database.UsersLink).ToList();

			var i = 0;
			foreach (var user in users)
			{
				i++;
				Console.WriteLine();
				Console.WriteLine("User #{0}", i);
				ViewUser(user);
			}

			Console.WriteLine();
			Console.WriteLine("Total users in database {0}: {1}", _database.Id, users.Count);
		}

		private static void ViewUser(User user)
		{
			Console.WriteLine("          User ID: {0} ", user.Id);
			Console.WriteLine("      Resource ID: {0} ", user.ResourceId);
			Console.WriteLine("        Self Link: {0} ", user.SelfLink);
			Console.WriteLine(" Permissions Link: {0} ", user.PermissionsLink);
			Console.WriteLine("        Timestamp: {0} ", user.Timestamp);
		}

		private async static Task<User> CreateUser(DocumentClient client, string userId)
		{
			Console.WriteLine();
			Console.WriteLine(">>> Create User {0} in {1} <<<", userId, _database.Id);

			var userDefinition = new User { Id = userId };
			var result = await client.CreateUserAsync(_database.SelfLink, userDefinition);
			var user = result.Resource;

			Console.WriteLine("Created new user");
			ViewUser(user);

			return user;
		}

		private async static Task DeleteUser(DocumentClient client, string userId)
		{
			Console.WriteLine();
			Console.WriteLine(">>> Delete User {0} in {1} <<<", userId, _database.Id);

			var query = new SqlQuerySpec
			{
				QueryText = "SELECT * FROM c WHERE c.id = @id",
				Parameters = new SqlParameterCollection { new SqlParameter { Name = "@id", Value = userId } }
			};

			User user = client.CreateUserQuery(_database.SelfLink, query).AsEnumerable().First();

			await client.DeleteUserAsync(user.SelfLink);

			Console.WriteLine("Deleted user {0} from database {1}", userId, _database.Id);
		}

		// Permissions

		private static void ViewPermissions(DocumentClient client, User user)
		{
			Console.WriteLine();
			Console.WriteLine(">>> View Permissions for {0} <<<", user.Id);

			var perms = client.CreatePermissionQuery(user.PermissionsLink).ToList();

			var i = 0;
			foreach (var perm in perms)
			{
				i++;
				Console.WriteLine();
				Console.WriteLine("Permission #{0}", i);
				ViewPermission(perm);
			}

			Console.WriteLine();
			Console.WriteLine("Total permissions for {0}: {1}", user.Id, perms.Count);
		}

		private static void ViewPermission(Permission perm)
		{
			Console.WriteLine("    Permission ID: {0} ", perm.Id);
			Console.WriteLine("      Resource ID: {0} ", perm.ResourceId);
			Console.WriteLine("  Permission Mode: {0} ", perm.PermissionMode);
			Console.WriteLine("	           Token: {0} ", perm.Token);
			Console.WriteLine("        Timestamp: {0} ", perm.Timestamp);
		}

		private async static Task CreatePermission(DocumentClient client, User user, string permId, PermissionMode permissionMode, string resourceLink)
		{
			Console.WriteLine();
			Console.WriteLine(">>> Create Permission {0} for {1} <<<", permId, user.Id);

			var permDefinition = new Permission { Id = permId, PermissionMode = permissionMode, ResourceLink = resourceLink };
			var result = await client.CreatePermissionAsync(user.SelfLink, permDefinition);
			var perm = result.Resource;

			Console.WriteLine("Created new permission");
			ViewPermission(perm);
		}

		private async static Task DeletePermission(DocumentClient client, User user, string permId)
		{
			Console.WriteLine();
			Console.WriteLine(">>> Delete Permission {0} from {1} <<<", permId, user.Id);

			var query = new SqlQuerySpec
			{
				QueryText = "SELECT * FROM c WHERE c.id = @id",
				Parameters = new SqlParameterCollection { new SqlParameter { Name = "@id", Value = permId } }
			};

			Permission perm = client.CreatePermissionQuery(user.PermissionsLink, query).AsEnumerable().First();

			await client.DeletePermissionAsync(perm.SelfLink);

			Console.WriteLine("Deleted permission {0} from user {1}", permId, user.Id);
		}

		private async static Task TestPermissions(DocumentClient client, User user, string collectionLink)
		{
			var perm = client.CreatePermissionQuery(user.PermissionsLink)
				.AsEnumerable()
				.First(p => p.ResourceLink == collectionLink);

			var resourceToken = perm.Token;

			dynamic documentDefinition = new
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

			Console.WriteLine();
			Console.WriteLine("Trying to create & delete document as user {0}", user.Id);
			try
			{
				var endpoint = ConfigurationManager.AppSettings["DocDbEndpoint"];
				using (var restrictedClient = new DocumentClient(new Uri(endpoint), resourceToken))
				{
					var document = await restrictedClient.CreateDocumentAsync(collectionLink, documentDefinition);
					Console.WriteLine("Successfully created document: {0}", document.Resource.id);

					await restrictedClient.DeleteDocumentAsync(document.Resource._self);
					Console.WriteLine("Successfully deleted document: {0}", document.Resource.id);
				}
			}
			catch (Exception ex)
			{
				Console.WriteLine("ERROR: {0}", ex.Message);
			}
		}

	}
}
