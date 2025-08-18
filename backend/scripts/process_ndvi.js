
var table = ee.FeatureCollection('projects/ee-brucepengyuan/assets/bei_jing');
var aoi = table.geometry().bounds();
var startDate = '2020-01-01';
var endDate = '2024-12-31';
var outputFolder = 'GEE_EVI_Data';
var targetScale = 2000;
var targetCRS = 'EPSG:32650';



var modisCollection = ee.ImageCollection('MODIS/061/MOD13A1')
  .filterDate(startDate, endDate)
  .filterBounds(aoi);

var maskAndScaleModis = function(image) {
  var qa = image.select('SummaryQA');
  var goodQuality = qa.bitwiseAnd(3).lte(1);

  var evi = image.select('EVI')
                 .multiply(0.0001)
                 .updateMask(goodQuality)
                 .rename('EVI');

  return evi.copyProperties(image, ['system:time_start']);
};

var processedModis = modisCollection.map(maskAndScaleModis);

var months = ee.List.sequence(0, ee.Date(endDate).difference(ee.Date(startDate), 'month').subtract(1));
var start_date_obj = ee.Date(startDate);

var monthlyEviCollection = ee.ImageCollection.fromImages(
  months.map(function(m) {
    var month_start = start_date_obj.advance(m, 'month');
    var month_end = month_start.advance(1, 'month');

    var monthlyMean = processedModis.filterDate(month_start, month_end).max();
    return monthlyMean.set('system:time_start', month_start.millis());
  })
);

print('Processed Monthly EVI Collection:', monthlyEviCollection);

Map.centerObject(aoi, 7);
var eviVisParam = {
  min: 0.0,
  max: 0.8,
  palette: [
    'FFFFFF', 'CE7E45', 'DF923D', 'F1B555', 'FCD163', '99B718',
    '74A901', '66A000', '529400', '3E8601', '207401', '056201',
    '004C00', '023B01', '012E01', '011D01', '011301'
  ]
};
var firstEviImage = monthlyEviCollection.first();
Map.addLayer(firstEviImage, eviVisParam, 'Monthly Mean EVI - First Month');
Map.addLayer(aoi, {color: 'FF0000', fillColor: '00000000'}, 'Beijing Bounding Box');


var year_to_export = '2021';

var collection_for_year = monthlyEviCollection.filter(
  ee.Filter.calendarRange(parseInt(year_to_export), parseInt(year_to_export), 'year')
);

var collectionList = collection_for_year.toList(collection_for_year.size());
var listSize = collectionList.size().getInfo();

for (var i = 0; i < listSize; i++) {
  var image = ee.Image(collectionList.get(i));
  var date = ee.Date(image.get('system:time_start')).format('YYYY_MM');
  var dateStr = date.getInfo();

  var fileName = 'EVI_Beijing_2km_UTM50N_' + dateStr;
  var taskDescription = 'Export_EVI_Beijing_2km_UTM50N_' + dateStr;

  Export.image.toDrive({
    image: image.toFloat(),
    description: taskDescription,
    folder: outputFolder,
    fileNamePrefix: fileName,
    region: aoi,
    scale: targetScale,
    crs: targetCRS,
    maxPixels: 1e13
  });
}