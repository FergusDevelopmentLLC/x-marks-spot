const fastcsv = require("fast-csv")
const { Client } = require("pg")
const request = require("request")
const states = require('./states.js').states

const isSuspicious = (rawArray) => {
  return rawArray.reduce((acc, column) => {
    if (!column.match(/^[0-9A-Za-z_]+$/)) {
      acc = true
    }
    return acc
  }, false)
}

const containsLongitudeLatitude = (rawArray) => {

  const caseInsensitiveFinder = (arrayToLookIn, strToFind) => (arrayToLookIn.findIndex(item => strToFind === item.toLowerCase()) > -1) ? true : false
  
  if(caseInsensitiveFinder(rawArray, 'longitude') && caseInsensitiveFinder(rawArray, 'latitude'))
    return true

  return false
}

const getCountyGeoJsonFor = (csvUrl, stateName) => {

  return new Promise(async (resolve, reject) => {
    
    const client = new Client()
    await client.connect()

    const targetTable = `csv_import_${ Date.now().toString() }`
    const inserts = []
    const csvData = []

    await fastcsv.parseStream(request(csvUrl))
      .on('error', error => reject(`Error occurred while parsing csv: ${error}`))
      .on('data', (data) => csvData.push(data))
      .on('end', async () => {
        
        let header = csvData.shift()//get the first header line

        if(!containsLongitudeLatitude(header)) {
          reject("Error: longitude, latitude colummns are required.")
          return
        }

        if(isSuspicious(header)) {
          reject("Error: suspicious data detected. Column names must be alphanumeric.")
          return
        }

        //build insert statements dynamically
        csvData.forEach((columnValues) => {
          let insert = `INSERT INTO ${ targetTable } (${header.map(value => value).join(',')}) VALUES (${header.map((value, i) => `$${i + 1}`)});`
          //into the inserts array, push the parameterized sql statement and the array of parameters
          inserts.push([insert, [...columnValues]])// example: ['INSERT INTO targetTable (col1,col2,col3) VALUES ($1,$2,$3)]', ['-121.225442','38.185269','Elaine Mary Dornton Dvm']
        })

        const columnsString = header.map(column => `${column} character varying`).join(",")

        //TODO, parameters don't seem to work with this sql
        await client.query(`CREATE TABLE ${targetTable} ( ${columnsString} ) WITH ( OIDS=FALSE );`)
          
        //TODO, parameters don't seem to work with this sql
        await client.query(`ALTER TABLE ${targetTable} OWNER TO geodevdb;`)

        inserts.forEach(async insert => await client.query(insert[0], insert[1]))

        const statefp = states.find(st => st.name === stateName).statefp

        const columnsStringWithPrefix = header.map(column => `max(geo_points.${column})`).join(",")
        const columnsStringWithoutPrefix = header.map(column => `${column}`).join(",")

        const sql = `
                    SELECT jsonb_build_object(
                      'type', 'FeatureCollection',
                      'features', jsonb_agg(features.feature)
                    )
                    FROM (
                      SELECT jsonb_build_object(
                      'type', 'Feature',
                      'geometry', ST_AsGeoJSON(geom,3)::jsonb,
                      'properties', to_jsonb(inputs) - 'geom'
                    ) AS feature
                    FROM 
                      (
                      SELECT 
                        county.geom,
                        ${columnsStringWithPrefix}
                        ,max(pop.type) as type,
                        COUNT(geo_points.geom) as geo_points_count,
                        MAX(pop.pop_2019) as pop_2019,
                        CASE 
                          WHEN count(geo_points.geom) = 0 THEN max(pop.pop_2019) 
                          ELSE max(pop.pop_2019) / count(geo_points.geom) 
                        END
                        AS persons_per_location
                      FROM county county
                      LEFT JOIN
                      (
                        SELECT 
                          ST_SetSRID(ST_MAKEPOINT(longitude::double precision, latitude::double precision),4326) as geom, 
                          ${ columnsStringWithoutPrefix }
                        FROM ${targetTable}
                      )
                      AS geo_points on ST_WITHIN(geo_points.geom, county.geom)
                      LEFT JOIN population_county pop on pop.name = county.name
                      WHERE county.statefp = $1
                      AND pop.statefp =  $1
                      GROUP BY county.geom, county.name
                      ORDER BY persons_per_location desc
                      ) inputs
                    ) features;`

        const geo = await client.query(sql, [statefp])

        await client.query(`DROP TABLE ${targetTable};`)

        await client.end()

        resolve(geo.rows[0]['jsonb_build_object'])
      })
  })
}

const getPointGeoJsonFor = (csvUrl, stateName) => {

  return new Promise(async (resolve, reject) => {
    
    const client = new Client()
    await client.connect()

    const targetTable = `csv_import_${ Date.now().toString() }`
    const inserts = []
    const csvData = []

    await fastcsv.parseStream(request(csvUrl))
      .on('error', error => reject(`Error occurred while parsing csv: ${error}`))
      .on('data', (data) => csvData.push(data))
      .on('end', async () => {
      
        let header = csvData.shift()//get the first header line

        if(!containsLongitudeLatitude(header)) {
          reject("Error: longitude, latitude colummns are required.")
          return
        }

        if(isSuspicious(header)) {
          reject("Error: suspicious data detected. Column names must be alphanumeric.")
          return
        }

        //build insert statements dynamically
        csvData.forEach((columnValues) => {
          let insert = `INSERT INTO ${ targetTable } (${header.map(value => value).join(',')}) VALUES (${header.map((value, i) => `$${i + 1}`)});`
          //into the inserts array, push the parameterized sql statement and the array of parameters
          inserts.push([insert, [...columnValues]])// example: ['INSERT INTO targetTable (col1,col2,col3) VALUES ($1,$2,$3)]', ['-121.225442','38.185269','Elaine Mary Dornton Dvm']
        })

        const columnsString = header.map(column => `${column} character varying`).join(",")
        
        await client.query(`CREATE TABLE ${targetTable} ( ${columnsString} ) WITH ( OIDS=FALSE );`)
          
        await client.query(`ALTER TABLE ${targetTable} OWNER TO geodevdb;`)
        
        inserts.forEach(async insert => await client.query(insert[0], insert[1]))//the inserts are parameterized

        const columnsStringWithPrefix = header.map(column => `geo_points.${column}`).join(",")
        const columnsStringWithoutPrefix = header.map(column => `${column}`).join(",")

        const sql = `
                    SELECT jsonb_build_object(
                      'type', 'FeatureCollection',
                      'features', jsonb_agg(features.feature)
                    )
                    FROM (
                      SELECT jsonb_build_object(
                      'type', 'Feature',
                      'geometry', ST_AsGeoJSON(geom,3)::jsonb,
                      'properties', to_jsonb(inputs) - 'geom'
                    ) AS feature
                    FROM 
                      (
                      
                        SELECT
                          geo_points.geom,
                          ${ columnsStringWithPrefix }
                        FROM state state
                        LEFT JOIN
                          (
                            SELECT 
                              ST_SetSRID(ST_MAKEPOINT(longitude::double precision, latitude::double precision),4326) as geom, 
                              ${ columnsStringWithoutPrefix }
                            FROM ${targetTable}
                          ) AS geo_points on ST_WITHIN(geo_points.geom, state.geom)
                        WHERE state.name = $1
                      
                      ) inputs
                    ) features;`
      
        const geo = await client.query(sql,[stateName])
        
        await client.query(`DROP TABLE ${targetTable};`)

        await client.end()

        resolve(geo.rows[0]['jsonb_build_object'])
    })
  })
}

module.exports = { getPointGeoJsonFor, getCountyGeoJsonFor }

