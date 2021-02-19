const sqlite3 = require('sqlite3')
const fastcsv = require("fast-csv")
const request = require("request")

const parseCsvToSqliteFrom = (url) => {

  console.log('parseCsv')

  const sqliteDbFileName = 'csv_import.db'

  const db = new sqlite3.Database(sqliteDbFileName)

  const targetTable = `csv_import_${ Date.now().toString() }`
  
  const inserts = []
  const csvData = []

  fastcsv.parseStream(request(url))
    .on('data', (data) => {
      csvData.push(data)
    })
    .on('end', () => {
      
      let header = csvData.shift()//get the first header line
      
      csvData.forEach((columnValues) => {
        let insert = `INSERT INTO ${ targetTable } (${header.map(value => value).join(', ')}) VALUES (${header.map(value => '?')})`
        inserts.push([insert, [...columnValues]])
      })

      const columns = header.map((value) => {
        return `${value} TEXT`
      }).join(",")
      
      const tableCreationSql = `create table if not exists ${targetTable} ( ${columns} )`
      
      db.run(tableCreationSql, () => {
        inserts.forEach((insert) => {
          db.run(insert[0], insert[1])
        })
        db.close()
      })
    })

}

module.exports = { parseCsvToSqliteFrom }