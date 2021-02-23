const express = require('express')
const bodyParser = require('body-parser')
const db = require('./db.js')
const app = express()

app.use(bodyParser.json())
app.use(bodyParser.urlencoded({ extended: true }))

app.get('/', (req, res, next) => {
  res.status(200).json(`Hello from nodejs-csv-pg. The current server date/time is: ${new Date()}`)
})

app.post('/getPointGeoJson', (req, res, next) => {

  const csvUrl = req.body.csvUrl
  const state = req.body.state

  db.getGeoJsonFor(csvUrl, state, 'point').then((geojson) => {
    res.status(200).json(geojson)
  },(error) => {
    res.status(200).json(error)
  })
  .catch((error) => {
    res.status(200).json(error)
  })

})

app.post('/getCountyGeoJson', async (req, res, next) => {

  const csvUrl = req.body.csvUrl
  const state = req.body.state

  db.getGeoJsonFor(csvUrl, state, 'county').then((geojson) => {
    res.status(200).json(geojson)
  },(error) => {
    res.status(200).json(error)
  })
  .catch((error) => {
    res.status(200).json(error)
  })

})

// app.post('/getCountyGeoJson', async (req, res, next) => {

//   try {

//     const csvUrl = req.body.csvUrl
//     const state = req.body.state

//     if(!isValid(state)) 
//       throw("Error: State name invalid")

//     const geojson = await db.getCountyGeoJsonFor(csvUrl, state)
//     res.status(200).json(geojson)
//   }
//   catch(error) {
//     res.status(200).json(error)
//   }

// })

const server = app.listen(4070, () => {
  console.log('App listening at port %s', server.address().port)
})
