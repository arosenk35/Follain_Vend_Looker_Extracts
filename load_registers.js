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

const TOKEN = "postgres";

console.log("Beginning upload");
//sequelize.query("truncate table follain.vend_raw_registers",function(err,rows){
//  if(err) throw exit(99);
//  console.log('raw table truncated');
//});

const files = fs.readdirSync(__dirname + "/files/registers").filter(f => f != ".DS_Store");

Promise.map(files, processFile, {concurrency: 20 });

function processFile(file){
    const data = JSON.parse(fs.readFileSync(__dirname + "/files/registers/" + file, "UTF-8"));

    if(data.body.registers){
        console.log(file + " : " + data.body.registers.length + " records");

        return Promise.all(data.body.registers.map(data => {
            return sequelize.query("with upsert as (  update follain.vend_raw_registers  set (data) = (:data)  where id=:id  returning *) insert into follain.vend_raw_registers  (id, data) select :id, :data where not exists (select 1 from upsert where upsert.id = :id);"
           , {replacements: {id: [data.id], data: [JSON.stringify(data)]}})
           .catch( err => {
                    console.log("got an error: ", err);

                })
        }));
    } else {
        return Promise.resolve([]);
    }


}

console.log("Completed loading registers");

