// =================================================================================
// 1. 定义参数 (与之前的脚本保持一致)
// =================================================================================

// 你的矢量文件Table ID
var table = ee.FeatureCollection('projects/ee-brucepengyuan/assets/bei_jing');
// 获取矢量文件的矩形范围 (Bounding Box)
var aoi = table.geometry().bounds();
// 定义时间范围
var startDate = '2020-01-01';
var endDate = '2024-12-31';
// 定义导出到Google Drive的文件夹名称
var outputFolder = 'GEE_EVI_Data'; // 新建一个文件夹用于存放EVI数据
// 定义你需要的最终分辨率 (2000米)
var targetScale = 2000;
// 定义目标投影坐标系
var targetCRS = 'EPSG:32650'; // WGS 1984 UTM Zone 50N

// =================================================================================
// 2. 加载和预处理 MODIS EVI 数据
// =================================================================================

// 加载MODIS 16天 500米 植被指数产品
var modisCollection = ee.ImageCollection('MODIS/061/MOD13A1')
  .filterDate(startDate, endDate)
  .filterBounds(aoi);

// 创建一个函数来掩膜掉低质量像素并应用比例因子
var maskAndScaleModis = function(image) {
  // 选择质量评估波段 (SummaryQA)
  var qa = image.select('SummaryQA');
  // SummaryQA 的 Bit 0-1 代表数据质量: 0=好, 1=可接受, 2=雪/冰, 3=云
  // 我们保留质量好和可接受的像素 (即 bit 0-1 <= 1)
  var goodQuality = qa.bitwiseAnd(3).lte(1);

  // 选择EVI波段，应用比例因子，并应用掩膜
  var evi = image.select('EVI')
                 .multiply(0.0001) // 应用比例因子 0.0001
                 .updateMask(goodQuality) // 应用质量掩膜
                 .rename('EVI');

  // 返回处理后的单波段影像，并保留时间属性
  return evi.copyProperties(image, ['system:time_start']);
};

// 将函数应用到整个影像集
var processedModis = modisCollection.map(maskAndScaleModis);

// =================================================================================
// 3. 创建月度影像集合 (Monthly Compositing)
// =================================================================================

// 生成需要处理的月份列表
var months = ee.List.sequence(0, ee.Date(endDate).difference(ee.Date(startDate), 'month').subtract(1));
var start_date_obj = ee.Date(startDate);

// 遍历每个月，计算月平均EVI
var monthlyEviCollection = ee.ImageCollection.fromImages(
  months.map(function(m) {
    // 计算当前月份的起始和结束日期
    var month_start = start_date_obj.advance(m, 'month');
    var month_end = month_start.advance(1, 'month');

    // 筛选出当月的影像，并计算平均值
    var monthlyMean = processedModis.filterDate(month_start, month_end).max();

    // 返回月平均影像，并设置正确的时间戳
    return monthlyMean.set('system:time_start', month_start.millis());
  })
);

// 打印检查处理后的月度影像集合
print('Processed Monthly EVI Collection:', monthlyEviCollection);

// =================================================================================
// 4. 可视化检查
// =================================================================================

Map.centerObject(aoi, 7);
// EVI 可视化参数，值越高代表植被越茂盛
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

// =================================================================================
// 5. 导出数据到 Google Drive (分批次)
// =================================================================================

// 同样，建议使用分批次导出的方法来避免客户端循环超时
// 您可以手动修改年份 '2020', '2021', '2022', '2023', '2024' 来分批运行
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
    scale: targetScale, // 2000
    crs: targetCRS,     // 'EPSG:32650'
    maxPixels: 1e13
  });
}