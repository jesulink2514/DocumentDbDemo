function spEnsureUniqueId(docToCreate) {

	var context = getContext();
	var collection = context.getCollection();
	var collectionLink = collection.getSelfLink();
	var response = context.getResponse();

	if (!docToCreate.id) {
		// Nothing to do if document has no ID; let DocumentDB generate a unique GUID
		createDocument();
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
					createDocument();
				}
				else {
					// Existing document found with this ID, generate a random suffix and test again
					docToCreate.id = baseId + generateRandom5();
					checkForDuplicate();
				}
			}
		);
		if (!isAccepted) {
			throw new Error('Timeout querying for duplicate document');
		}
	}

	function createDocument() {
		var isAccepted = collection.createDocument(collectionLink, docToCreate, {},
			function (err, docCreated) {
				if (err) {
					throw new Error('Error creating document with id "' + docToCreate.id + '". ' + err.message);
				}
				response.setBody(docCreated);
			}
		);
		if (!isAccepted) {
			throw new Error('Timeout creating new document');
		}
	}

	function generateRandom5() {
		var text = "";
		var possible = "0123456789";

		for (var i = 0; i < 5; i++) {
			text += possible.charAt(Math.floor(Math.random() * possible.length));
		}

		return text;
	}

}
