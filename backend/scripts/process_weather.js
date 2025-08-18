var table = ee.FeatureCollection('projects/ee-brucepengyuan/assets/bei_jing');
var aoi = table.geometry().bounds();
var startDate = '2025-01-01';
var endDate = '2025-7-31';
var outputFolder = 'GEE_TerraClimate_Data';
var targetScale = 2000;


var terraClimate = ee.ImageCollection('IDAHO_EPSCOR/TERRACLIMATE')
  .filterBounds(aoi)
  .filterDate(startDate, endDate)
  .select([
    'tmmx',
    'tmmn',
    'pr',
    'vs',
    'vap'
  ]);

print('Filtered TerraClimate Collection:', terraClimate);

var preprocessTerraClimate = function(image) {
  var tmmx = image.select('tmmx');
  var tmmn = image.select('tmmn');
  var pr = image.select('pr');
  var vs = image.select('vs');
  var vap = image.select('vap');

  var mean_temp = tmmx.add(tmmn).divide(2).multiply(0.1)
                      .rename('temperature_C');

  var precip_mm = pr.rename('precipitation_mm');

  var wind_speed = vs.multiply(0.01).rename('wind_speed_ms');

  var vapor_pressure = vap.multiply(0.001)
                         .rename('vapor_pressure_kPa');

  return ee.Image.cat([
    mean_temp,
    precip_mm,
    wind_speed,
    vapor_pressure
  ]).copyProperties(image, ['system:time_start']);
};

var processedCollection = terraClimate.map(preprocessTerraClimate);

print('Processed Collection:', processedCollection);

Map.centerObject(aoi, 7);
var tempVisParam = {
  min: -20,
  max: 30,
  palette: ['blue', 'cyan', 'yellow', 'red']
};
var firstImage = processedCollection.first();
Map.addLayer(firstImage.select('temperature_C'), tempVisParam, 'Mean Temperature (°C)');
Map.addLayer(aoi, {color: 'FF0000', fillColor: '00000000'}, 'Beijing Bounding Box');

var year_to_export = '2024';

var collection_for_year = processedCollection.filter(
  ee.Filter.calendarRange(parseInt(year_to_export), parseInt(year_to_export), 'year')
);

var collectionList = collection_for_year.toList(collection_for_year.size());
var listSize = collectionList.size().getInfo();

for (var i = 0; i < listSize; i++) {
  var image = ee.Image(collectionList.get(i));
  var date = ee.Date(image.get('system:time_start')).format('YYYY_MM');
  var dateStr = date.getInfo();

  var fileName = 'TerraClimate_Beijing_2km_UTM50N_' + dateStr;
  var taskDescription = 'Export_TC_Beijing_2km_UTM50N_' + dateStr;

  Export.image.toDrive({
    image: image.toFloat(),
    description: taskDescription,
    folder: outputFolder,
    fileNamePrefix: fileName,
    region: aoi,
    scale: targetScale,
    crs: 'EPSG:32650',
    maxPixels: 1e13
  });
}