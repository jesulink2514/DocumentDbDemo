using DocDb.DotNetSdk.Demos;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace DocDb.DotNetSdk
{
	public static class Program
	{
		private static IDictionary<string, Func<Task>> DemoMethods;

		private static void Main(string[] args)
		{
			DemoMethods = new Dictionary<string, Func<Task>>();
			DemoMethods.Add("DB", DatabasesDemo.Run);
			DemoMethods.Add("CO", CollectionsDemo.Run);
			DemoMethods.Add("DO", DocumentsDemo.Run);
			DemoMethods.Add("SP", StoredProceduresDemo.Run);
			DemoMethods.Add("UF", UserDefinedFunctionsDemo.Run);
			DemoMethods.Add("TR", TriggersDemo.Run);
			DemoMethods.Add("UP", UsersAndPermissionsDemo.Run);
			DemoMethods.Add("IR", IdBasedRoutingDemo.Run);
			DemoMethods.Add("IX", IndexingDemo.Run);
			DemoMethods.Add("AT", AttachmentsDemo.Run);
			DemoMethods.Add("C", Cleanup.Run);

			Task.Run(async () =>
			{
				ShowMenu();
				while (true)
				{
					Console.Write("Selection: ");
					var input = Console.ReadLine();
					var demoId = input.ToUpper().Trim();
					if (DemoMethods.Keys.Contains(demoId))
					{
						var demoMethod = DemoMethods[demoId];
						await RunDemo(demoMethod);
					}
					else if (demoId == "Q")
					{
						break;
					}
					else
					{
						Console.WriteLine("?{0}", input);
					}
				}
			}).Wait();
		}

		private static void ShowMenu()
		{
			Console.WriteLine(@"DocumentDB .NET SDK demos

DB Databases
CO Collections
DO Documents
SP Stored procedures
UF User defined functions
TR Triggers
UP Users & Permissions
IR ID-based routing
IX Indexing
AT Attachments

C  Cleanup

Q  Quit
");
		}

		private async static Task RunDemo(Func<Task> demoMethod)
		{
			try
			{
				await demoMethod();
			}
			catch (Exception ex)
			{
				var message = ex.Message;
				while (ex.InnerException != null)
				{
					ex = ex.InnerException;
					message += Environment.NewLine + ex.Message;
				}
				Console.WriteLine("Error: {0}", ex.Message);
			}
			Console.WriteLine();
			Console.Write("Done. Press any key to continue...");
			Console.ReadKey(true);
			Console.Clear();
			ShowMenu();
		}

	}
}
