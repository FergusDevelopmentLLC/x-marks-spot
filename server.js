const express = require('express')
const bodyParser = require('body-parser')
const pgUtils = require('./pgUtils.js')
const states = require('./states.js').states

const app = express()
app.use(bodyParser.json())
app.use(bodyParser.urlencoded({ extended: true }))

const isValid = (stateName) => {
  return states.reduce((acc, st) => {

    if(states.find(st => st.name === stateName))
      acc = true
    
    return acc
  }, false)
}

app.get('/', (req, res, next) => {
  res.status(200).json(`Hello from nodejs-csv-pg. The current server date/time is: ${new Date()}`)
})

app.post('/getPointGeoJson', async (req, res, next) => {
  
  try 
  {
    const csvUrl = req.body.csvUrl
    const state = req.body.state
    
    if(!isValid(state)) 
      throw("Error: State name invalid")

    const geojson = await pgUtils.getPointGeoJsonFor(csvUrl, state)
    res.status(200).json(geojson)
  }
  catch(error) {
    res.status(200).json(error)
  }

})

app.post('/getCountyGeoJson', async (req, res, next) => {

  try {

    const csvUrl = req.body.csvUrl
    const state = req.body.state

    if(!isValid(state)) 
      throw("Error: State name invalid")

    const geojson = await pgUtils.getCountyGeoJsonFor(csvUrl, state)
    res.status(200).json(geojson)
  }
  catch(error) {
    res.status(200).json(error)
  }

})

const server = app.listen(4070, () => {
  console.log('App listening at port %s', server.address().port)
})
