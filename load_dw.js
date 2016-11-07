const fs = require('fs');
const Sequelize = require('sequelize');
const Promise = require('bluebird');
const http = require('request');
const _ = require('lodash');
Promise.promisifyAll(http);

const nconf = require('nconf');
nconf.argv()
 // .env() 
 //.file('config', { file: 'config/' + process.env.NODE_ENV + '.json' })
  .file('config', { file: __dirname + "/config/" + 'prod' + '.json' }); 
  
  var connectionInfo = {
  connectionString: nconf.get('connectionString')
    };
console.log("Starting sequelize " + connectionInfo.connectionString);
const sequelize = new Sequelize(connectionInfo.connectionString, {
    dialect: 'postgres',
    logging: false,
    multipleStatements: true,
    pool: {
        max: 5,
        min: 0,
        idle: 10000
    },
    define: {
        timestamps: true,
        paranoid: true,
        underscoredAll: true,
        underscored: true
    }
});
var sql = fs.readFileSync(__dirname + "/sql/load_warehouse_production.sql").toString()
    .replace(/(\r\n|\n|\r)/gm," ") // remove newlines
    .replace(/\s+/g, ' ') // excess white space
   // .split(";") // split into all statements
   // .map(Function.prototype.call, String.prototype.trim)
   // .filter(function(el) {return el.length != 0}); // remove any empty ones
    ;
console.log(sql)
const TOKEN = "postgres";


console.log("Beginning DW upload");
sequelize.query(sql,function(err,rows){
  if(err) throw exit(99);
  console.log('DW loaded');
});

