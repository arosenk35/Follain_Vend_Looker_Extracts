const fs = require('fs');
const nconf = require('nconf');
const Promise = require('bluebird');
const http = require('request');
const _ = require('lodash');
const log = require('winston');
Promise.promisifyAll(http);

nconf.argv()
 // .env() 
 //.file('config', { file: 'config/' + process.env.NODE_ENV + '.json' })
  .file('config', { file: __dirname + "/config/" + 'prod' + '.json' }); 
  
  var connectionInfo = {
  domainPrefix: nconf.get('domain_prefix'),
  accessToken: nconf.get('access_token'),
  refreshToken: nconf.get('refresh_token'), // oauth.txt
  vendTokenService: nconf.get('vend:token_service'), // config/<env>.json
  vendClientId: nconf.get('vend:client_id'), // config/<env>.json
  vendClientSecret: nconf.get('vend:client_secret') // config/<env>.json
};


var getTokenUrl = function(tokenService, domain_prefix) {
  var tokenUrl = tokenService.replace(/\{DOMAIN_PREFIX\}/, domain_prefix);
  log.debug('token Url: '+ tokenUrl);
  return tokenUrl;
};


var refreshAccessToken = function(tokenService, clientId, clientSecret, refreshToken, domainPrefix) {
  log.debug('refreshAccessToken - token service: ' + tokenService);
  log.debug('refreshAccessToken - client Id: ' + clientId);
  log.debug('refreshAccessToken - client Secret: ' + clientSecret);
  log.debug('refreshAccessToken - refresh token: ' +  refreshToken);
  log.debug('refreshAccessToken - domain prefix: ' + domainPrefix);

  if ( !(tokenService && clientId && clientSecret && refreshToken) ) {
    return Promise.reject('missing required arguments for refreshAccessToken()');
  }

  var tokenUrl = getTokenUrl(tokenService, domainPrefix);
 
  var options = {
    url: tokenUrl,
    headers: {
      'Accept': 'application/x-www-form-urlencoded'
    },
    form: {
      'grant_type': 'refresh_token',
      'client_id': clientId,
      'client_secret': clientSecret,
      'refresh_token': refreshToken
    },json:true
  };
    
   return  http.post(options,function(error, response, oauth)
   {  
     console.log('received new access_token: ' + oauth.access_token);
          connectionInfo.accessToken = oauth.access_token; 
          nconf.set('access_token',oauth.access_token);
          nconf.save(function (err) {
          if (err) {
            console.error(err.message);
            return;
            }
           console.log('Configuration saved successfully.');
            });
   })};

refreshAccessToken( 
      connectionInfo.vendTokenService,
      connectionInfo.vendClientId,
      connectionInfo.vendClientSecret,
      connectionInfo.refreshToken,
      connectionInfo.domainPrefix)

setTimeout(function() {

var sincedate=new Date();
sincedate.setHours( sincedate.getHours() - 120 )
var sinceDateC=sincedate.toISOString().replace(/T/, ' ').replace(/\..+/, '');

getvenddata("outlets",connectionInfo,1);
getvenddata("registers",connectionInfo,1);
getvenddata("products",connectionInfo,25);
getvenddata("stock_movements",connectionInfo,90);
getvenddatasince("register_sales",connectionInfo,50,sinceDateC);
//getvenddata("customers",connectionInfo,420);
getvenddata("supplier",connectionInfo,5);
//getvenddata("consignment",connectionInfo,420);
//getvenddata("register_sales",connectionInfo,1300);
}, 5000);



function getvenddata(api_name,connectionInfo,nbrpages){
var page = _.range(nbrpages).map(x => x+1);
Promise.map(page, fetchpages, {concurrency: 1 }).then( x => {
    console.log(api_name+" COMPLETE");})

function fetchpages(page){
    console.log(api_name+" page "+ page);
    return http.getAsync({
        url: "https://follain.vendhq.com/api/"+api_name,
        qs: {
            page: page
        },
        headers: { "Authorization": "Bearer " + connectionInfo.accessToken },
        json:true
    }).then( result => {
        console.log( api_name+" Response " + page, result);
        fs.writeFileSync(__dirname + "/files/"+api_name+"/"+api_name+"_" + page+ ".json", JSON.stringify(result), "UTF-8");
    });
}}
function getvenddatasince(api_name,connectionInfo,nbrpages,sinceDateC){
var page = _.range(nbrpages).map(x => x+1);

rmDir(__dirname + "/files/"+api_name+"/",false);

Promise.map(page, fetchpages, {concurrency: 1 }).then( x => {
    console.log(api_name+" COMPLETE");});

function fetchpages(page){
    console.log(api_name+" page "+ page);
       return http.getAsync({
        url: "https://follain.vendhq.com/api/"+api_name,
        qs: {
            page: page, 
            since: sinceDateC 
        },
        headers: { "Authorization": "Bearer " + connectionInfo.accessToken },
        json:true
    }).then( result => {
        console.log( api_name+" Response " + page, result);
        fs.writeFileSync(__dirname + "/files/"+api_name+"/"+api_name+"_" + page+ ".json", JSON.stringify(result), "UTF-8");
    });
}}

rmDir = function(dirPath, removeSelf) {
      if (removeSelf === undefined)
        removeSelf = true;
      try { var files = fs.readdirSync(dirPath); }
      catch(e) { return; }
      if (files.length > 0)
        for (var i = 0; i < files.length; i++) {
          var filePath = dirPath + '/' + files[i];
          if (fs.statSync(filePath).isFile())
            fs.unlinkSync(filePath);
          else
            rmDir(filePath);
        }
      if (removeSelf)
        fs.rmdirSync(dirPath);
    };