function spBulkInsert(docs) {

	if (!docs) {
		throw new Error('Documents array is null or undefined.');
	}

	var context = getContext();
	var collection = context.getCollection();
	var response = context.getResponse();

	var docCount = docs.length;

	if (docCount == 0) {
		response.setBody(0);
		return;
	}

	var count = 0;

	createDoc(docs[0]);

	function createDoc(doc) {
		var isAccepted = collection.createDocument(collection.getSelfLink(), doc, docCreated);
		if (!isAccepted) {
			response.setBody(count);
		}
	}

	function docCreated(err, doc) {
		if (err) {
			throw err;
		}
		count++;
		if (count == docCount) {
			response.setBody(count);
		}
		else {
			createDoc(docs[count]);
		}
	}
}
