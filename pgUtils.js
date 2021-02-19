const fastcsv = require("fast-csv")
const request = require("request")
const { Client } = require("pg")

const states = [
  {statefp : "01", name : "Alabama"},
  {statefp : "02", name : "Alaska"},
  {statefp : "04", name : "Arizona"},
  {statefp : "05", name : "Arkansas"},
  {statefp : "06", name : "California"},
  {statefp : "08", name : "Colorado"},
  {statefp : "09", name : "Connecticut"},
  {statefp : "10", name : "Delaware"},
  {statefp : "11", name : "District of Columbia"},
  {statefp : "12", name : "Florida"},
  {statefp : "13", name : "Georgia"},
  {statefp : "15", name : "Hawaii"},
  {statefp : "16", name : "Idaho"},
  {statefp : "17", name : "Illinois"},
  {statefp : "18", name : "Indiana"},
  {statefp : "19", name : "Iowa"},
  {statefp : "20", name : "Kansas"},
  {statefp : "21", name : "Kentucky"},
  {statefp : "22", name : "Louisiana"},
  {statefp : "23", name : "Maine"},
  {statefp : "24", name : "Maryland"},
  {statefp : "25", name : "Massachusetts"},
  {statefp : "26", name : "Michigan"},
  {statefp : "27", name : "Minnesota"},
  {statefp : "28", name : "Mississippi"},
  {statefp : "29", name : "Missouri"},
  {statefp : "30", name : "Montana"},
  {statefp : "31", name : "Nebraska"},
  {statefp : "32", name : "Nevada"},
  {statefp : "33", name : "New Hampshire"},
  {statefp : "34", name : "New Jersey"},
  {statefp : "35", name : "New Mexico"},
  {statefp : "36", name : "New York"},
  {statefp : "37", name : "North Carolina"},
  {statefp : "38", name : "North Dakota"},
  {statefp : "39", name : "Ohio"},
  {statefp : "40", name : "Oklahoma"},
  {statefp : "41", name : "Oregon"},
  {statefp : "42", name : "Pennsylvania"},
  {statefp : "44", name : "Rhode Island"},
  {statefp : "45", name : "South Carolina"},
  {statefp : "46", name : "South Dakota"},
  {statefp : "47", name : "Tennessee"},
  {statefp : "48", name : "Texas"},
  {statefp : "49", name : "Utah"},
  {statefp : "50", name : "Vermont"},
  {statefp : "51", name : "Virginia"},
  {statefp : "53", name : "Washington"},
  {statefp : "54", name : "West Virginia"},
  {statefp : "55", name : "Wisconsin"},
  {statefp : "56", name : "Wyoming"},
  {statefp : "72", name : "Puerto Rico"}
]

const getCountyGeoJsonFor = (csvUrl, stateName) => {

  return new Promise(async (resolve) => {
    
    const client = new Client()
    await client.connect()

    const targetTable = `csv_import_${ Date.now().toString() }`
    const inserts = []
    const csvData = []

    await fastcsv.parseStream(request(csvUrl))
    .on('data', (data) => {
      csvData.push(data)
    })
    .on('end', async () => {
      
      let header = csvData.shift()//get the first header line

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

      console.log('sql', sql)
      
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
    .on('data', (data) => {
      csvData.push(data)
    })
    .on('end', async () => {
      
      let header = csvData.shift()//get the first header line

      let badData = false
      header.forEach((column) => {
        if (!column.match(/^[0-9A-Za-z_]+$/)) {
          badData = true
        }
      })
      
      if(badData) {
        reject("bad data detected.")
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
                          FROM public.${targetTable}
                        ) AS geo_points on ST_WITHIN(geo_points.geom, state.geom)
                      WHERE state.name = $1'
                    
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

