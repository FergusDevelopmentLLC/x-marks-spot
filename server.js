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
  const csvUrl = req.body.csvUrl//TODO:validate
  const state = req.body.state//TODO:validate
  
  if(!isValid(state)) {
    res.status(200).json("State name invalid")
    return
  }

  try {
    const geojson = await pgUtils.getPointGeoJsonFor(csvUrl, state)
    res.status(200).json(geojson)
  }
  catch(err) {
    res.status(200).json(err)
  }
  
})

app.post('/getCountyGeoJson', async (req, res, next) => {
  const csvUrl = req.body.csvUrl//TODO:validate
  const state = req.body.state//TODO:validate

  if(!isValid(state)) {
    res.status(200).json("State name invalid")
    return
  }

  try {
    const geojson = await pgUtils.getCountyGeoJsonFor(csvUrl, state)
    res.status(200).json(geojson)
  }
  catch(err) {
    res.status(200).json(err)
  }

})

const server = app.listen(4070, () => {
  console.log('App listening at port %s', server.address().port)
})

// https://gist.githubusercontent.com/FergusDevelopmentLLC/2d2ef2fe6bf41bb7f10cb7a87efbb803/raw/1aaea6621e64892fd1fc9642bb14a729c892ffe8/animal_hospitals_ca.csv
// https://gist.githubusercontent.com/FergusDevelopmentLLC/f0ea84b0d4d50604c405f5b74db1b498/raw/863d21945fe993f224ee8ad3771930568568a7c3/category.csv