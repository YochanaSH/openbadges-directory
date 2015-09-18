var cloudant = require('cloudant'),
request       = require('request'),
logger        = require('../logger'),
config        = require('../config');

var dbCredentials = {
		dbName : 'cloudant_badge_classes'
};

var db;
var badge_index;

// This method implements CloudantQuery Search
function createSearchIndex () {
	console.log("In Create Index >>> ");
	request({
		method: 'POST',
		json: true,
		uri: dbCredentials.url + '/' + dbCredentials.dbName +'/_index',
		body: {
				"index": {},
				"name" : "badge-index",
				"type" : "text"
			}
	}, function (err, response, body) {
		badge_index = response.body.id;
		if (err && err.message === 'Not Found') {
			console.log(">>"+err.message);
		}
	});
}
	
function initDBConnection() {
	if(process.env.VCAP_SERVICES) {
		var vcapServices = JSON.parse(process.env.VCAP_SERVICES);
		if(vcapServices.cloudantNoSQLDB) {
			dbCredentials.host = vcapServices.cloudantNoSQLDB[0].credentials.host;
			dbCredentials.port = vcapServices.cloudantNoSQLDB[0].credentials.port;
			dbCredentials.user = vcapServices.cloudantNoSQLDB[0].credentials.username;
			dbCredentials.password = vcapServices.cloudantNoSQLDB[0].credentials.password;
			dbCredentials.url = vcapServices.cloudantNoSQLDB[0].credentials.url;
		}
		console.log('VCAP Services: '+JSON.stringify(process.env.VCAP_SERVICES));
	}
	else{
		dbCredentials.host = "ca455560-ca2e-4346-adbb-20acc3adbb70-bluemix.cloudant.com";
		dbCredentials.port = 443;
		dbCredentials.user = "ca455560-ca2e-4346-adbb-20acc3adbb70-bluemix";
		dbCredentials.password = "efbcc2fef9bbce27946219421911ef342494da1be9702815c24c01e0578d92a6";
		dbCredentials.url = "https://ca455560-ca2e-4346-adbb-20acc3adbb70-bluemix:efbcc2fef9bbce27946219421911ef342494da1be9702815c24c01e0578d92a6@ca455560-ca2e-4346-adbb-20acc3adbb70-bluemix.cloudant.com";

	}

	cloudant = require('cloudant')(dbCredentials.url);
	var check = false;
	//check if DB exists if not create
	cloudant.db.list(function (error, body) {
		if(body != null && body.length == 0) check = true;
		body.forEach(function(db) {
		    if(db === dbCredentials.dbName) 
		    	console.log("Database "+dbCredentials.dbName+" Exists");
		    else
		    	check = true;
		  });
		  
		  if(error) console.log("Error listing database");

		  if(check) {
	    	cloudant.db.create(dbCredentials.dbName, function (err, res) 
			{
				//After database creation, we can switch between CloudantQuery
	    		if(res) { console.log("in create"); createSearchIndex(); }
	    		// or Cloudant based Lucene Search
	    		//if(res) { console.log("in create"); createDesignDocument(); }
				if (err) { console.log('could not create db ',err); }
			});
	      }
	});
	this.db = cloudant.use(dbCredentials.dbName);	
}

function CloudantSearch() {
	initDBConnection();
}

function createDesignDocument() {
	var cloudantquery = {
		"_id":dbCredentials.dbName, 
		indexes: {"searchAll": 
				  {
				   "index":"function(doc){ if(doc._id) { index('id',doc._id,{'store': true}); } if(doc.source.name) { index('name',doc.source.name,{'store': true}); } if(doc.source._directory._valid) { index('valid',doc.source._directory._valid,{'store': true}); }}"
				  }
				}
	};
	request({
		method: 'PUT', 
		uri: dbCredentials.url + '/' + dbCredentials.dbName+'/_design/'+dbCredentials.dbName, 
		json: true, 
		body: cloudantquery
	}, function (error, response, body) {
		logger.info(response);
		if (err && err.message === 'Not Found') {
			return callback(null, null);
		}
		return callback(err,response);
	});	

}
function decorateResponse(cloudantDoc) {
	// if (cloudantDoc.fields && cloudantDoc.fields._timestamp) {
		// cloudantDoc.source._directory._timestamp = cloudantDoc.fields._timestamp;
	// }
	return cloudantDoc.source;
}

CloudantSearch.prototype.get = function get(id, callback) {
	var cloudantquery = {
			"selector": {
				"_id": {"$eq": id}
			}
	};
	request({
		method: 'POST', 
		uri: dbCredentials.url + '/' + dbCredentials.dbName+'/_find', 
		json: true, 
		body: cloudantquery
	}, function (err, response) {
		result = response.body.docs;
		console.log("GET >>>> "+response.statusCode);
		if (err && err.message === 'Not Found') {
			return callback(null, null);
		}
		return callback(err, response.body.docs);
	});	
};

CloudantSearch.prototype.indexExists = function (callback) {
	request({
		method: 'GET',
		uri: dbCredentials.url + '/' +dbCredentials.dbName + '/_index/badge-index'
	}, function (error, response) {
		if (error) {
			return callback(error);
		}
		if (response && response.found && response.statusCode !== 200) {
			return callback(null, false);
		}
		return callback(null, true);
	});
};

CloudantSearch.prototype.index = function (document, callback) {
	var data = {"_id":document._directory._location, "source":document} ;
	request({
		method: 'POST', 
		uri: dbCredentials.url + '/' + dbCredentials.dbName,
		json: true, 
		body: data
	}, function (error, response, body) {
		if (error && error.message === 'Not Found') {
			return callback(null, null);
		}
		return callback(error, response && response.found && decorateResponse(response));
	});	
};

// REVIST THIS LATER
CloudantSearch.prototype.createIndex = function (callback) {
	request({
		method: 'POST',
		uri: dbCredentials.url + '/' + dbCredentials.dbName +'/_index',
		body: {
			"_id": "badge_class",
		   "indexes": { 
		        "id": ["_id"],
		        "store":true
		    },
		}		
	}, function (error, response) {
		if (response.statusCode !== 200) {
			return callback(new Error('There was an issue creating the index'));
		}
		logger.info('Response from create index: ', response.body);
		return callback(null);
	});};

// TO DO
CloudantSearch.prototype.issuers = function (options, callback) {
	var limit = options.limit || 0;
	request({
		method: 'POST', 
		uri: dbCredentials.url + '/' + dbCredentials.dbName +'/badge_class/_search',  
		json: true, 
		body: {
			"selector": {
				aggs: {
					issuersResolved: {
						terms: {
							field: 'issuerResolved',
							size: limit
						}
					}
				}
			}	
		}
	}, function (error, response) {
		if (error || response.statusCode !== 200) {
			return callback(new Error('There was an problem getting issuers from the index'));
		}
		return callback(null, response.body.aggregations.issuerResolved.buckets.map(function (bucket) {
			console.log('ISSUERS '+ JSON.stringify(bucket));
			var byKey = {};
			byKey.tag = bucket.key;
			byKey.count = bucket.doc_count;
			return byKey;
		}));
	});	
};

CloudantSearch.prototype.searchData = function searchData(options, callback) {
	searchOptions= {'selector': options,'fields': ['_id','_rev']}
	console.log("searchOptions @@@@@@@@@@@ "+JSON.stringify(searchOptions));
	request({
		method: 'POST', 
		uri: dbCredentials.url + '/' + dbCredentials.dbName +'/_find',
		json: true, 
		body: searchOptions
	}, function (err, response) {
			if (err) {
			console.log(">>>>>>>>>>>>"+err.message);
		}

		console.log("Search #### "+response.statusCode+response.body);
		return callback(err,response.body);

	});	
};

CloudantSearch.prototype.search = function search(options, callback) {
	var limit     = options.limit || 10,
	page          = options.page,
	searchOptions= '"selector": {'
	searchOptions+= '"source._directory._valid":'+true
	
	if (options.q) {
		searchOptions+=',"'+'$text'+'":"'+ options.q + '*"'
			// query_string: {
				// query: options.q + '*'
			// }
		//});
	}
	// if (options.tags) {
		// console.log("tags >>"+options.tags);
		// searchOptions.body.query.bool.must.push({
			
			// query_string: {
				// default_field: 'tags',
				// query: options.tags.map(function (tag) { return tag + '*'; }).join(' AND ')
			// }
		//});
	//}
	// if (options.name) {
		// searchOptions.body.query.bool.must.push({
			// query_string: {
				// default_field: 'name',
				// query: options.name + '*'
			// }
		// });
	// }
	// if (options.issuer) {
		// searchOptions.body.query.bool.must.push({
			// query_string: {
				// default_field: 'issuerResolved.name',
				// query: options.issuer + '*'
			// }
		// });
	// }
	// if (!options.q && !options.tags && !options.name && !options.issuer) {
		//searchOptions.sort = '_timestamp:desc';
	// }
	searchOptions+='}'
	bodyContent = '{ "limit":'+limit+','+searchOptions+'}'
	request({
		method: 'POST', 
		uri: dbCredentials.url + '/' + dbCredentials.dbName +'/_find',
		json: true, 
		body: bodyContent
	}, function (err, response) {
		console.log("Search "+response.statusCode);
		callback(err,response.body.docs);

	});	
};

// TO DO
CloudantSearch.prototype._invalidBadges = function (options, callback) {
	var limit         = 1, //options.limit || 10,
	page          = options.page,
	searchOptions = {
			index: 'badge_classes',
			type: 'badge_class',
			from: (page && ((limit * page) - limit)) || 0,
			size: limit,
			fields: ['_timestamp', 'source'],
			body: {
				query: {
					bool: {
						must: [{
							term: {
								'_directory._valid': false
							}
						}]
					}
				}
			}
	};

	if (!options.q && !options.tags) {
		searchOptions.sort = '_timestamp:desc';
	}
	this.search(searchOptions, function (err, response) {
		callback(err,
				response &&
				response.hits &&
				response.hits.hits.map(function (hit) {
					return decorateResponse(hit);
				}));
	});
};

// TO DO
CloudantSearch.prototype.tags = function (options, callback) {
	var limit = options.limit || 0;
	request({
		method: 'POST', 
		uri: dbCredentials.url + '/' + dbCredentials.dbName +'badge_class/_find', 
		json: true, 
		body: { "selector": {
				aggs: {
					tags: {
						terms: {
							field: 'tags',
							size: limit
						}
					}
				}	        	
			}
		}
	}, function (error, response, body) {
		if (error || response.statusCode !== 200) {
			return callback(new Error('There was an issue getting tags from the index'));
		}
		return callback(null, response.body.aggregations.tags.buckets.map(function (bucket) {
			var byKey = {};
			byKey.tag = bucket.key;
			byKey.count = bucket.doc_count;
			return byKey;
		}));
	});
};

//TO DO
CloudantSearch.prototype.refresh = function (callback) {
	request({
		method: 'POST', 
		uri: dbCredentials.url + '/' + dbCredentials.dbName //+'/_refresh'
	}, function (error, response, body) {
		if (response.statusCode !== 200) {
			return callback(new Error('There was an issue refreshing the index: ' + body));
		}
		return callback(null, body);
	});
};

/** WARNING - Deletes Entire Index **/
//TESTING PENDING
CloudantSearch.prototype.deleteIndex = function (callback) {
console.log("deleteIndex>>>>>>>>>>>>>> ");
	//curl -XDELETE 'http://localhost:9200/twitter/'
	request({
		method: 'DELETE',
		uri: dbCredentials.url + '/' + dbCredentials.dbName
	}, function (error, response, body) {
		if (response.statusCode !== 200) {
			return callback(new Error('There was an issue deleting the index: ' + body));
		}
		return callback(null, body);
	});
};

//TESTING PENDING
CloudantSearch.prototype.deleteByEndpoint = function (endpoint, callback) {
	console.log("deleteByEndpoint >>>>>>>>>>>>>> "+JSON.stringify(endpoint));
	//return this._deleteByQuery({'source._directory._endpoint':endpoint}, callback);
	return this._deleteByQuery({
		query: {
			bool: {
				must: [{
					query_string: {
						default_field: '_directory._endpoint',
						query: endpoint
					}
				}]
			}
		}
	}, callback);	
};

//TESTING PENDING
CloudantSearch.prototype.deleteById = function (id, callback) {
	console.log("deleteById >>>>>>>>>>>>>> "+id);
	request({
		method: 'DELETE',
		uri: dbCredentials.url + '/' + dbCredentials.dbName + '/badge_class/' + encodeURIComponent(id)
	}, function (error, response, body) {
		if (response.statusCode !== 200) {
			return callback(new Error('There was an issue deleting the index: ' + body));
		}
		return callback(null, body);
	});
};

//TESTING PENDING
CloudantSearch.prototype._deleteByQuery = function (query, callback) {
	request({
		method: 'DELETE',
		uri: dbCredentials.url + '/' + dbCredentials.dbName + '/badge_class/_query',
		body: JSON.stringify(query)
	}, function (err, response) {
		if (response.statusCode !== 200) {
			return callback(new Error('There was an issue deleting the index: '));
		}
		if (err && err.message === 'Not Found') {
			console.log(">>"+err.message);
		}		
		return callback(null, body);
	});
	// var data;
	// console.log("deleteByQuery >>>>>>>>>>>>>> "+JSON.stringify(query));
	// deleteData = this.searchData(query, function (err, results) {
      // if (err) {
        // return next(new restify.InternalServerError(err.message));
      // }
      // data: results;
	   // if (results && results.length) {
		 // results.forEach(function (item) { console.log(item+"################# "); });
	  // } else {
		// console.log(results+"############# "+JSON.stringify(results));
	  // }
	  
    // });
	   // if (data && data.length) {
		 // data.forEach(function (item) { console.log(item+"################# "); });
	  // } else {
		// console.log("%%%%% "+JSON.stringify(data));
	  // }
	//console.log("@@@@@@@@"+deleteData._id+"@@@@"+deleteData._rev);
	
  // request({
    // method: 'DELETE',
    // uri: dbCredentials.url + '/' + dbCredentials.dbName+'/_bulk_docs'

  // }, function (error, response, body) {
    // if (response.statusCode !== 200) {
      // return callback(new Error('There was an issue deleting the index: ' + body));
    // }
    // return callback(null, body);
  // });
};


module.exports = CloudantSearch;