function spSelectCount(filterQuery, continuationToken) {

	var context = getContext();
	var collection = context.getCollection();
	var response = context.getResponse();

	var pageSize = 200;
	var maxCount = 600;

	var count = 0;

	getCount(continuationToken);

	function getCount(nextContinuationToken) {
		var responseOptions = { continuation: nextContinuationToken, pageSize: pageSize };
		if (count >= maxCount || !getDocuments(responseOptions)) {
			exitProcedure(nextContinuationToken);
		}
	}

	function getDocuments(responseOptions) {
		var isAccepted = (filterQuery && filterQuery.length) ?
            collection.queryDocuments(collection.getSelfLink(), filterQuery, responseOptions, gotDocuments) :
            collection.readDocuments(collection.getSelfLink(), responseOptions, gotDocuments);
		return isAccepted;
	}

	function gotDocuments(err, docFeed, options) {
		if (err) {
			throw 'Error counting documents: ' + err;
		}
		count += docFeed.length;
		if (options.continuation) {
			getCount(options.continuation);
		}
		else {
			exitProcedure(null);
		}
	}

	function exitProcedure(continuationToken) {
		var body = {
			count: count,
			continuationToken: continuationToken
		};
		response.setBody(body);
	}
}
