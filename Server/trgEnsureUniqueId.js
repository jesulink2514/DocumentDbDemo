function trgEnsureUniqueId() {

	var context = getContext();
	var collection = context.getCollection();
	var collectionLink = collection.getSelfLink();
	var request = context.getRequest();
	var docToCreate = request.getBody();

	if (!docToCreate.id) {
		// Nothing to do if document has no ID; let DocumentDB generate a unique GUID
		return;
	}

	var baseId = docToCreate.id;

	checkForDuplicate();

	function checkForDuplicate() {
		var query = {
			query: 'SELECT VALUE c.id FROM c WHERE c.id = @id',
			parameters: [{ name: '@id', value: docToCreate.id }]
		};

		var isAccepted = collection.queryDocuments(collectionLink, query, {},
			function (err, results) {
				if (err) {
					throw new Error('Error querying for duplicate document: ' + err.message);
				}
				if (results.length == 0) {
					// No existing document found with this ID, create the new document and exit
					request.setBody(docToCreate);
				}
				else {
					docToCreate.id = baseId + generateRandom5();
					checkForDuplicate();
				}
			}
		);
		if (!isAccepted) {
			throw new Error('Timeout querying for duplicate document');
		}
	}

	function generateRandom5() {
		var text = "";
		var possible = "0123456789";

		for (var i = 0; i < 5; i++)
			text += possible.charAt(Math.floor(Math.random() * possible.length));

		return text;
	}
}
