function trgUpdateMetadata() {

	var context = getContext();
	var collection = context.getCollection();
	var collectionLink = collection.getSelfLink();
	var response = context.getResponse();

	var createdDoc = response.getBody();

	var metadataDocument;

	collection.queryDocuments(collectionLink, 'SELECT * FROM c WHERE c.id = "_metadata"', {},
		function (err, results) {
			if (err) {
				throw new Error('Error querying for metadata document: ' + err.message);
			}
			if (results.length == 1) {
				metadataDocument = results[0];
				updateMetadataDocument();
			}
			else {
				collection.createDocument(collectionLink, { id: "_metadata" }, {},
					function (err, createdMetadataDocument) {
						if (err) {
							throw new Error('Error creating metadata document: ' + err.message);
						}
						metadataDocument = createdMetadataDocument;
						updateMetadataDocument();
					}
				);
			}
		}
	);

	function updateMetadataDocument() {
		metadataDocument.lastId = createdDoc.id;
		collection.replaceDocument(metadataDocument._self, metadataDocument,
			function (err) {
				if (err) {
					throw new Error('Error updating metadata document: ' + err.message);
				}
			}
		);
	}

}
