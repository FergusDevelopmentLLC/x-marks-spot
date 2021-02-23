const fastcsv = require("fast-csv")
const { Client } = require("pg")
const format = require('pg-format');
const request = require("request")
const states = require('./states.js').states
const utils = require('./utils.js')

const getGeoJsonFor = (csvUrl, stateName, type) => {

  return new Promise((resolve, reject) => {
    
    const targetTableName = `csv_import_${ Date.now().toString() }`
    const insertData = []
    const csvData = []

    const client = new Client()
    
    client.connect()
      .then(() => {
        
        fastcsv.parseStream(request(csvUrl))
          .on('error', error => reject(`Error occurred while parsing csv: ${error}`))
          .on('data', (data) => csvData.push(data))
          .on('end', () => {

            if(!utils.validateState(stateName)){
              reject(`Error: State name is invalid`)
              return
            }

            let csvError = utils.validateCsvData(csvData)
            if(csvError) {
              reject(`Error: ${csvError}`)
              return
            }
              
            const header = csvData.shift()//the first line in the csv are the columns
            
            csvData.forEach(columnValues => insertData.push([...columnValues]))

            const columnsString = header.map(column => `${column} character varying`).join(",")

            client.query(`CREATE TABLE ${targetTableName} ( ${columnsString} ) WITH ( OIDS=FALSE );`)
              .then(() => {
                
                client.query(`ALTER TABLE ${targetTableName} OWNER TO geodevdb;`)
                  .then(() => { 

                    const insertStatements = format(`INSERT INTO ${targetTableName} (longitude, latitude, name) VALUES %L`, insertData)
                    
                    client.query(insertStatements)
                      .then(() => {

                        const statefp = states.find(st => st.name === stateName).statefp
                        const columnsStringWithoutPrefix = header.map(column => `${column}`).join(",")

                        let columnsStringWithPrefix
                        let stateArray
                        
                        if(type == 'county') {
                          columnsStringWithPrefix = header.map(column => `max(geo_points.${column})`).join(",")
                          stateArray = [statefp]
                        }
                        else {
                          columnsStringWithPrefix = header.map(column => `geo_points.${column}`).join(",")
                          stateArray = [stateName]
                        }
                        
                        let geoSQL = utils.getGeoSQL(type) 

                        geoSQL = geoSQL
                                  .replace('#columnsStringWithPrefix', columnsStringWithPrefix)
                                  .replace('#columnsStringWithoutPrefix', columnsStringWithoutPrefix)
                                  .replace('#targetTableName', targetTableName)

                        client.query(geoSQL, stateArray)
                          .then((geoResult) => {
                            resolve(geoResult.rows[0]['jsonb_build_object'])
                          })
                          .catch((error) => {
                            reject(error)
                            return
                          })
                      })
                      .catch((error) => {
                        reject(error)
                        return
                      })

                  })
                  .catch((error) => {
                    reject(error)
                    return
                  }) 
              })
              .catch((error) => {
                reject(error)
                return
              })
          })
      
      })
      .catch((error) => {
        reject("Database connection error")
        return
      })
  
  })
}

module.exports = { getGeoJsonFor }