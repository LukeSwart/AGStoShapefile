// @Author: Joshua Tanner
// @Date: 12/8/2014 (created)
// @Description: Easy way to create shapefiles (and geojson, geoservices json)
//               from ArcGIS Server services
// @services.txt format :: serviceLayerURL|layerName
// @githubURL : https://github.com/tannerjt/AGStoShapefile
// Modifications: 8/4/2016 by Tamas Kramer (KT)
//   -- split large shapefiles to smaller ones
//   -- fixed a bug that caused incomplete shapefiles
// Node Modules
var ogr2ogr = require('ogr2ogr');
var esri2geo = require('esri2geo');
var q = require('q');
var request = q.nfbind(require('request'));
var objectstream = require('objectstream');
var fs = require('fs');
var queryString = require('query-string');
var winston = require('winston');
var async = require('async');

// Setup logging with winston
winston.level = 'debug';
// winston.add(winston.transports.File, {filename: './logfile.log'});

// ./mixin.js
// merge user query params with default
var mixin = require('./mixin');

var serviceFile = process.argv[2] || 'services.txt';
var outDir = process.argv[3] || './output/';
if (outDir[outDir.length - 1] !== '/') {
    outDir += '/';
}

// Make request to each service
fs.readFile(serviceFile, function(err, data) {
    if (err) {
        winston.info(err);
        throw err;
    }
    data.toString().split('\n').forEach(function(service) {
        var service = service.split('|');
        if (service[0].split('').length == 0) return;
        var baseUrl = getBaseUrl(service[0].trim()) + '/query';

        var reqQS = {
            where: '1=1',
            returnIdsOnly: true,
            f: 'json'
        };
        var userQS = getUrlVars(service[0].trim());
        // mix one obj with another
        var qs = mixin(userQS, reqQS);
        var qs = queryString.stringify(qs);
        var url = decodeURIComponent(getBaseUrl(baseUrl) + '/query/?' + qs);

        request({
            url: url,
            method: 'GET',
            json: true
        }, function(err, response, body) {
            var err = err || body.error;
            if (err) {
                winston.info(err);
                throw err;
            }
            requestService(service[0].trim(), service[1].trim(), body.objectIds);
        });
    })
});

// Request JSON from AGS
function requestService(serviceUrl, serviceName, objectIds) {
    objectIds.sort();
    winston.info('Number of features for service: ', objectIds.length);
    // KT 2016-08-04 begin
    // KT L_chunk = 100 caused error in at least one test case, had to decrease to e.g. 20.
    const L_chunk = 20;
    const n_chunk = Math.ceil(objectIds.length / L_chunk);
    // KT Split to files of max. 50 chunks = 1000 entities
    const n_chunk_per_file = 50;
    const L_file = n_chunk_per_file * L_chunk;
    const n_file = Math.ceil(objectIds.length / L_file);
    // KT 2016-08-04 end

    winston.info('Getting chunks of ' + L_chunk + ' features...');

    // KT 2016-08-04 begin
    var i_file = 0;
    async.whilst(function() {
            return i_file < n_file;
        },
        function(next) {

            var requests = [];

            // KT Bounding chunk # for the current file
            var i_chunk0 = i_file * n_chunk_per_file;
            var i_chunk1 = i_chunk0 + n_chunk_per_file;
            if (i_chunk1 > n_chunk) {
                i_chunk1 = n_chunk
            }
            for (var i_chunk = i_chunk0; i_chunk < i_chunk1; i_chunk++) {
                var ids = [];

                // KT Bounding entity # for the current file
                //  fixed a bug in J. Tanner's version
                //  that caused the last chunk to be incomplete
                var j0 = i_chunk * L_chunk
                var j1 = j0 + L_chunk
                if (j1 > objectIds.length) {
                    j1 = objectIds.length
                }

                ids = objectIds.slice(j0, j1);
                // KT 2016-08-04 end

                if (ids[0] !== undefined) {
                    winston.info('query ->', j0, 'out of', objectIds.length);
                } else {
                    winston.info('wait for requests to settle...');
                    continue;
                }

                // we need these query params
                var reqQS = {
                    objectIds: ids.join(','),
                    geometryType: 'esriGeometryEnvelope',
                    returnGeometry: true,
                    returnIdsOnly: false,
                    outFields: '*',
                    outSR: '4326',
                    f: 'json'
                };
                // user provided query params
                var userQS = getUrlVars(serviceUrl);
                // mix one obj with another
                var qs = mixin(userQS, reqQS);
                var qs = queryString.stringify(qs);
                var url = decodeURIComponent(getBaseUrl(serviceUrl) + '/query/?' + qs);
                var r = request({
                    url: url,
                    method: 'GET',
                    json: true
                });

                requests.push(r);
            }; // for i_chunk

            q.allSettled(requests).then(function(results) {
                winston.info('all requests settled');
                var allFeatures;
                for (var i = 0; i < results.length; i++) {
                    if (results[i].value !== undefined) {
                        if (i == 0) {
                            allFeatures = results[i].value[0].body;
                        } else {
                            allFeatures.features =
                                allFeatures.features.concat(results[i].value[0].body.features);
                        }
                    } else {
                        winston.info('undefined feature #' + i + ' in file ' + i_file);
                    }
                }

                var fname = serviceName;
                var fname_file = serviceName + (n_file > 1 ? ("_" + pad(i_file, 4)) : "");

                winston.info('creating', fname_file, 'json');
                var json = allFeatures;
                //allFeatures = undefined;

                //esri json
                winston.info('Creating Esri JSON');
                var path_json = outDir + fname_file + '.json';
                var stream = fs.createWriteStream(path_json);
                var objstream = objectstream.createSerializeStream(stream);
                objstream.write(json);
                objstream.end();

                //geojson
                winston.info('Creating GeoJSON');
                var path_geojson = outDir + fname_file + '.geojson';
                var stream = fs.createWriteStream(path_geojson);
                var objstream = objectstream.createSerializeStream(stream);
                esri2geo(json, function(err, data) {
                    if (err) {
                        throw (err);
                        winston.info('Error converting esri json to geojson');
                        return;
                    }
                    objstream.write(data);
                    objstream.end();

                    // winston.info('Creating Shapefile');
                    // var path_shp = outDir + fname_file + '.zip';

                    // KT 2016-08-04 begin
                    // KT -- I had to disable geojson --> shp conversion here
                    // and use ogr2ogr afterwards from the command line instead, because
                    // of synchronisation issues. Even so, some wms had to be
                    // downloaded in a separate serviceFile, not bulked together
                    // with other wms.

                    /*
                    winston.info('Creating Shapefile');
                    //shapefile
                    var shapefile = ogr2ogr(path_geojson)
                    	.format('ESRI Shapefile')
                    	.skipfailures();
                    //shapefile.stream().pipe(fs.createWriteStream(path_shp));
                    // KT 2016-08-04 end
                    */
                }); // esri2geo

                i_file++;
                next();
            }).catch(function(err) {
                winston.info(err);
                throw err;
            }); // q.allsettled
        },
        function(err) {
            // All things are done!
        }); // async
}


//http://stackoverflow.com/questions/4656843/jquery-get-querystring-from-url
function getUrlVars(url) {
    var vars = {},
        hash;
    var hashes = url.slice(url.indexOf('?') + 1).split('&');
    for (var i = 0; i < hashes.length; i++) {
        hash = hashes[i].split('=');
        vars[hash[0].toString()] = hash[1];
    }
    return vars;
}

// get base url for query
function getBaseUrl(url) {
    // remove any query params
    var url = url.split("?")[0];
    if ((/\/$/ig).test(url)) {
        url = url.substring(0, url.length - 1);
    }
    return url;
}

// pad suffix with character z
// http://stackoverflow.com/questions/10073699/pad-a-number-with-leading-zeros-in-javascript
function pad(n, width, z) {
    z = z || '0';
    n = n + '';
    return n.length >= width ? n : new Array(width - n.length + 1).join(z) + n;
}
